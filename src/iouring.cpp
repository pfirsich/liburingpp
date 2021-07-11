#include "iouring.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstring>

#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

// [1]: https://kernel.dk/io_uring.pdf

namespace {
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

void readBarrier()
{
    // [1]: "Ensure previous writes are visible before doing subsequent memory reads"
    std::atomic_thread_fence(std::memory_order_acquire);
}

void writeBarrier()
{
    // [1]: Order this write after previous writes.
    std::atomic_thread_fence(std::memory_order_release);
}
}

void IoURing::cleanup()
{
    if (sqrPtr_) {
        ::munmap(sqrPtr_, sqrSize_);
        sqrPtr_ = nullptr;
    }
    if (cqrPtr_ && sqrPtr_ != cqrPtr_) {
        ::munmap(cqrPtr_, cqrSize_);
        cqrPtr_ = nullptr;
    }
    if (sqes_) {
        ::munmap(sqes_, sqEntries_ * sizeof(io_uring_sqe));
        sqes_ = nullptr;
    }
    if (ringFd_ != -1) {
        ::close(ringFd_);
        ringFd_ = -1;
    }
}

IoURing::~IoURing()
{
    cleanup();
}

bool IoURing::init(size_t sqEntries)
{
    assert(ringFd_ == -1);

    // https://manpages.debian.org/unstable/liburing-dev/io_uring_setup.2.en.html
    assert(isPowerOfTwo(sqEntries));
    io_uring_params params;
    ::memset(&params, 0, sizeof(params));
    ringFd_ = io_uring_setup(sqEntries, &params);
    if (ringFd_ == -1) {
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
        cleanup();
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
            cleanup();
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
        cleanup();
        return false;
    }

    return true;
}

io_uring_cqe* IoURing::peekCqe(unsigned* numAvailable) const
{
    assert(ringFd_ != -1);
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

io_uring_cqe* IoURing::waitCqe(size_t num) const
{
    assert(ringFd_ != -1);
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

void IoURing::advanceCq(size_t num) const
{
    assert(ringFd_ != -1);
    // Is this fence enough? (I am reading head again)
    *cqHeadPtr_ = *cqHeadPtr_ + num;
    writeBarrier();
}

io_uring_sqe* IoURing::getSqe()
{
    assert(ringFd_ != -1);
    readBarrier();
    const auto head = *sqHeadPtr_;
    if (sqesTail_ - head < sqEntries_) {
        const auto idx = sqesTail_ & (*sqRingMaskPtr_);
        sqesTail_++;
        return sqes_ + idx;
    }
    return nullptr;
}

size_t IoURing::flushSqes(size_t num)
{
    assert(ringFd_ != -1);
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

void IoURing::submitSqes(size_t num, size_t waitCqes)
{
    assert(ringFd_ != -1);
    io_uring_enter(ringFd_, flushSqes(num), waitCqes, waitCqes > 0 ? IORING_ENTER_GETEVENTS : 0);
}

io_uring_sqe* IoURing::prepareReadv(int fd, const iovec* iov, int iovcnt, off_t offset)
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
