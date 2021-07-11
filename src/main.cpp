#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

#include <fcntl.h>
#include <linux/io_uring.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include "fd.hpp"

constexpr size_t BlockSize = 4096;

// [1]: https://kernel.dk/io_uring.pdf

int io_uring_setup(unsigned entries, io_uring_params* p)
{
    return (int)syscall(__NR_io_uring_setup, entries, p);
}
int io_uring_enter(
    int ring_fd, unsigned int to_submit, unsigned int min_complete, unsigned int flags)
{
    return (int)syscall(__NR_io_uring_enter, ring_fd, to_submit, min_complete, flags, NULL, 0);
}

template <typename T>
constexpr bool isPowerOfTwo(T v)
{
    return v != 0 && (v & (v - 1)) == 0;
}

template <typename T>
void clear(T* p)
{
    std::memset(p, 0, sizeof(T));
}

void readBarrier()
{
    // [1]: "Ensure previous writes are visible before doing subsequent memory reads"
    std::atomic_thread_fence(std::memory_order_acquire);
}

void writeBarrier()
{
    // [2]: Order this write after previous writes.
    std::atomic_thread_fence(std::memory_order_release);
}

// THIS CLASS IS NOT THREAD-SAFE.
// One could think about making it thread-safe, but it would introduce and
// extra-cost that would have to be paid, even if it is used from a single thread. Also
// there are a few cases, which are hard to resolve cleanly and efficiently, e.g.:
// * Thread 1 gets a CQE, thread 2 gets a CQE, thread 2 finishes the processing first:
//   we would have to delay the moving of the CQ head until thread 1 is also done.
//   Generalizing this to many threads makes the problem worse of course.
// Similar problems arise with submissions, though those could be resolved more easily.
class IoURing {
public:
    ~IoURing()
    {
        if (sqrPtr_) {
            ::munmap(sqrPtr_, sqrSize_);
        }
        if (cqrPtr_ && sqrPtr_ != cqrPtr_) {
            ::munmap(cqrPtr_, cqrSize_);
        }
        if (sqes_) {
            ::munmap(sqes_, sqEntries_ * sizeof(io_uring_sqe));
        }
    }

    bool init(size_t sqEntries = 128)
    {
        // https://manpages.debian.org/unstable/liburing-dev/io_uring_setup.2.en.html
        assert(isPowerOfTwo(sqEntries));
        io_uring_params params;
        clear(&params);
        ringFd_.reset(io_uring_setup(sqEntries, &params));
        if (ringFd_ < -1) {
            return false;
        }
        sqEntries_ = params.sq_entries;

        auto sqrSize_ = params.sq_off.array + params.sq_entries * sizeof(unsigned);
        auto cqrSize_ = params.cq_off.cqes + params.cq_entries * sizeof(io_uring_cqe);

        if (params.features & IORING_FEAT_SINGLE_MMAP) {
            sqrSize_ = std::max(sqrSize_, cqrSize_);
            cqrSize_ = sqrSize_;
        }

        sqrPtr_ = static_cast<uint8_t*>(::mmap(nullptr, sqrSize_, PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE, ringFd_, IORING_OFF_SQ_RING));
        if (sqrPtr_ == MAP_FAILED) {
            return false;
        }

        sqHeadPtr_ = reinterpret_cast<unsigned*>(sqrPtr_ + params.sq_off.head);
        sqTailPtr_ = reinterpret_cast<unsigned*>(sqrPtr_ + params.sq_off.tail);
        sqRingMaskPtr_ = reinterpret_cast<unsigned*>(sqrPtr_ + params.sq_off.ring_mask);
        sqIndexArray_ = reinterpret_cast<unsigned*>(sqrPtr_ + params.sq_off.array);

        if (params.features & IORING_FEAT_SINGLE_MMAP) {
            cqrPtr_ = sqrPtr_;
        } else {
            cqrPtr_ = static_cast<uint8_t*>(::mmap(nullptr, cqrSize_, PROT_READ | PROT_WRITE,
                MAP_SHARED | MAP_POPULATE, ringFd_, IORING_OFF_CQ_RING));
            if (cqrPtr_ == MAP_FAILED) {
                return false;
            }
        }

        cqHeadPtr_ = reinterpret_cast<unsigned*>(cqrPtr_ + params.cq_off.head);
        cqTailPtr_ = reinterpret_cast<unsigned*>(cqrPtr_ + params.cq_off.tail);
        cqRingMaskPtr_ = reinterpret_cast<unsigned*>(cqrPtr_ + params.cq_off.ring_mask);
        cqes_ = reinterpret_cast<io_uring_cqe*>(cqrPtr_ + params.cq_off.cqes);

        sqes_ = static_cast<io_uring_sqe*>(::mmap(nullptr, params.sq_entries * sizeof(io_uring_sqe),
            PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ringFd_, IORING_OFF_SQES));
        if (sqes_ == MAP_FAILED) {
            return false;
        }

        return true;
    }

    io_uring_cqe* peekCqe(unsigned* numAvailable = nullptr)
    {
        const auto head = *cqHeadPtr_;
        readBarrier();
        const auto tail = *cqTailPtr_;
        assert(tail >= head);
        const auto available = tail - head;
        if (numAvailable) {
            *numAvailable = available;
        }
        if (available > 0) {
            const auto idx = head & *cqRingMaskPtr_;
            const auto cqe = cqes_ + idx;
            return cqe;
        }
        return nullptr;
    }

    io_uring_cqe* waitCqe(size_t num = 1)
    {
        assert(num > 0);
        unsigned numAvailable = 0;
        auto cqe = peekCqe(&numAvailable);
        if (num <= numAvailable) {
            return cqe;
        }
        const auto res = io_uring_enter(ringFd_, 0, num, IORING_ENTER_GETEVENTS);
        if (res < 0) {
            return nullptr;
        }
        cqe = peekCqe();
        assert(cqe);
        return cqe;
    }

    void advanceCq(size_t num = 1)
    {
        // Is this fence enough? (I am reading head again)
        *cqHeadPtr_ = *cqHeadPtr_ + num;
        writeBarrier();
    }

    struct CQEHandle {
        IoURing* ring = nullptr;
        io_uring_cqe* cqe = nullptr;

        CQEHandle(IoURing* ring, io_uring_cqe* cqe)
            : ring(ring)
            , cqe(cqe)
        {
        }

        CQEHandle(const CQEHandle&) = delete;
        CQEHandle& operator=(const CQEHandle&) = delete;

        CQEHandle(CQEHandle&& other)
            : ring(other.ring)
            , cqe(other.cqe)
        {
            other.release();
        }

        CQEHandle& operator=(CQEHandle&& other)
        {
            reset();
            ring = other.ring;
            cqe = other.cqe;
            other.release();
            return *this;
        }

        void reset()
        {
            if (ring && cqe) {
                ring->advanceCq();
            }
            release();
        }

        void release()
        {
            ring = nullptr;
            cqe = nullptr;
        }

        ~CQEHandle()
        {
            reset();
        }
    };

    io_uring_sqe* getSqe()
    {
        readBarrier();
        const auto head = *sqHeadPtr_;
        if (sqesTail_ - head < sqEntries_) {
            const auto idx = sqesTail_ & (*sqRingMaskPtr_);
            sqesTail_++;
            return sqes_ + idx;
        }
        return nullptr;
    }

    size_t flushSqes(size_t num = 0)
    {
        const auto sqesGotten = sqesTail_ - sqesHead_;
        assert(num <= sqesGotten);
        const auto toFlush = num == 0 ? sqesGotten : std::min(num, sqesGotten);
        if (toFlush == 0) {
            return 0;
        }

        const auto mask = *sqRingMaskPtr_;
        auto tail = *sqTailPtr_;
        for (size_t i = 0; i < toFlush; ++i) {
            sqIndexArray_[tail & mask] = sqesHead_ & mask;
            tail++;
            sqesHead_++;
        }
        writeBarrier();
        *sqTailPtr_ = tail;
        writeBarrier();

        return toFlush;
    }

    void submitSqes(size_t num = 0, size_t waitCqes = 0)
    {
        io_uring_enter(
            ringFd_, flushSqes(num), waitCqes, waitCqes > 0 ? IORING_ENTER_GETEVENTS : 0);
    }

    io_uring_sqe* prepareReadv(int fd, const iovec* iov, int iovcnt, off_t offset = 0)
    {
        const auto sqe = getSqe();
        sqe->opcode = IORING_OP_READV;
        sqe->flags = 0;
        sqe->ioprio = 0;
        sqe->fd = fd;

        sqe->off = offset;
        sqe->addr = reinterpret_cast<uint64_t>(iov);
        sqe->len = iovcnt;

        sqe->rw_flags = 0;
        sqe->user_data = 0;
        sqe->__pad2[0] = sqe->__pad2[1] = sqe->__pad2[2] = 0;
        return sqe;
    }

private:
    Fd ringFd_;

    size_t sqEntries_ = 0;

    uint8_t* sqrPtr_ = nullptr;
    size_t sqrSize_ = 0;
    unsigned* sqHeadPtr_ = nullptr;
    unsigned* sqTailPtr_ = nullptr;
    unsigned* sqRingMaskPtr_ = nullptr;
    unsigned* sqIndexArray_ = nullptr;

    uint8_t* cqrPtr_ = nullptr;
    size_t cqrSize_ = 0;
    unsigned* cqHeadPtr_ = nullptr;
    unsigned* cqTailPtr_ = nullptr;
    unsigned* cqRingMaskPtr_ = nullptr;
    io_uring_cqe* cqes_ = nullptr;

    io_uring_sqe* sqes_ = nullptr;
    size_t sqesHead_ = 0;
    size_t sqesTail_ = 0;
};

int main(int argc, char** argv)
{
    std::vector<std::string> args(argv, argv + argc);
    if (argc < 2) {
        std::cerr << "Usage: " << args[0] << " FILE..." << std::endl;
        return 1;
    }

    IoURing ring;
    ring.init();

    struct File {
        std::string name;
        Fd fd;
        size_t size = 0;
        std::vector<iovec> iovecs = {};
    };

    auto getNumBlocks
        = [](size_t size) { return size / BlockSize + (size % BlockSize > 0 ? 1 : 0); };

    std::vector<File> files;
    files.reserve(argc - 1);
    for (int i = 1; i < argc; ++i) {
        File file { args[i], ::open(args[i].c_str(), O_RDONLY) };
        if (file.fd < 0) {
            std::cerr << "Could not open '" << file.name << std::endl;
            return 1;
        }

        struct stat st;
        if (::fstat(file.fd, &st) < 0) {
            std::cerr << "Could not stat file '" << file.name << std::endl;
            return 1;
        }
        if (!S_ISREG(st.st_mode)) {
            std::cerr << "'" << file.name << "' is not a regular file" << std::endl;
            return 1;
        }
        file.size = st.st_size;

        const auto numBlocks = getNumBlocks(file.size);
        file.iovecs.resize(numBlocks);
        for (size_t block = 0; block < numBlocks; ++block) {
            if (::posix_memalign(&file.iovecs[block].iov_base, BLOCK_SIZE, BLOCK_SIZE) != 0) {
                std::cerr << "Error in posix_memalign" << std::endl;
                return 1;
            }
            file.iovecs[block].iov_len = file.size - block * BLOCK_SIZE;
        }

        const auto sqe = ring.prepareReadv(file.fd, file.iovecs.data(), file.iovecs.size());
        sqe->user_data = files.size();
        files.push_back(std::move(file));
    }
    ring.submitSqes();
    if (!ring.waitCqe(files.size())) {
        std::cerr << "Error waiting for CQEs: "
                  << std::make_error_code(static_cast<std::errc>(errno)).message() << std::endl;
        return 1;
    }

    for (const auto& file : files) {
        const auto cqe = ring.peekCqe();
        if (cqe->res < 0) {
            std::cerr << "Error reading file '" << file.name << "': " << cqe->res << std::endl;
            return 1;
        }
        const auto numBlocks = getNumBlocks(file.size);
        for (size_t block = 0; block < numBlocks; ++block) {
            std::cout << std::string_view(
                static_cast<const char*>(file.iovecs[block].iov_base), file.iovecs[block].iov_len);
        }
        ring.advanceCq();
    }
}
