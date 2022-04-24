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

IoURing::CQEHandle::CQEHandle(const IoURing* ring, uint64_t userData, int32_t res)
    : ring(ring)
    , userData(userData)
    , res(res)
{
}

IoURing::CQEHandle::CQEHandle(CQEHandle&& other)
    : ring(other.ring)
    , userData(other.userData)
    , res(other.res)
{
    other.release();
}

IoURing::CQEHandle& IoURing::CQEHandle::operator=(CQEHandle&& other)
{
    finish();
    ring = other.ring;
    userData = other.userData;
    res = other.res;
    other.release();
    return *this;
}

void IoURing::CQEHandle::finish()
{
    if (ring) {
        ring->advanceCq();
    }
    release();
}

void IoURing::CQEHandle::release()
{
    ring = nullptr;
}

IoURing::CQEHandle::~CQEHandle()
{
    finish();
}

void IoURing::cleanup()
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
    if (ringFd_ != -1) {
        ::close(ringFd_);
    }
    release();
}

void IoURing::release()
{
    sqrPtr_ = nullptr;
    cqrPtr_ = nullptr;
    sqes_ = nullptr;
    ringFd_ = -1;
}

IoURing::~IoURing()
{
    cleanup();
}

bool IoURing::init(size_t sqEntries)
{
    assert(ringFd_ == -1);

    // https://manpages.debian.org/unstable/liburing-dev/io_uring_setup.2.en.html
    if (!isPowerOfTwo(sqEntries) || sqEntries < 1 || sqEntries > 4096) {
        return false;
    }

    ::memset(&params_, 0, sizeof(params_));
    ringFd_ = io_uring_setup(sqEntries, &params_);
    if (ringFd_ == -1) {
        return false;
    }

    sqEntries_ = params_.sq_entries;

    auto sqrSize_ = params_.sq_off.array + params_.sq_entries * sizeof(unsigned);
    // cq_entries is usually 2 * sq_entries
    auto cqrSize_ = params_.cq_off.cqes + params_.cq_entries * sizeof(io_uring_cqe);

    if (params_.features & IORING_FEAT_SINGLE_MMAP) {
        sqrSize_ = std::max(sqrSize_, cqrSize_);
        cqrSize_ = sqrSize_;
    }

    sqrPtr_ = static_cast<uint8_t*>(::mmap(nullptr, sqrSize_, PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_POPULATE, ringFd_, IORING_OFF_SQ_RING));
    if (sqrPtr_ == MAP_FAILED) {
        cleanup();
        return false;
    }

    sqHeadPtr_ = reinterpret_cast<unsigned*>(sqrPtr_ + params_.sq_off.head);
    sqTailPtr_ = reinterpret_cast<unsigned*>(sqrPtr_ + params_.sq_off.tail);
    sqRingMaskPtr_ = reinterpret_cast<unsigned*>(sqrPtr_ + params_.sq_off.ring_mask);
    sqIndexArray_ = reinterpret_cast<unsigned*>(sqrPtr_ + params_.sq_off.array);

    if (params_.features & IORING_FEAT_SINGLE_MMAP) {
        cqrPtr_ = sqrPtr_;
    } else {
        cqrPtr_ = static_cast<uint8_t*>(::mmap(nullptr, cqrSize_, PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE, ringFd_, IORING_OFF_CQ_RING));
        if (cqrPtr_ == MAP_FAILED) {
            cleanup();
            return false;
        }
    }

    cqHeadPtr_ = reinterpret_cast<unsigned*>(cqrPtr_ + params_.cq_off.head);
    cqTailPtr_ = reinterpret_cast<unsigned*>(cqrPtr_ + params_.cq_off.tail);
    cqRingMaskPtr_ = reinterpret_cast<unsigned*>(cqrPtr_ + params_.cq_off.ring_mask);
    cqes_ = reinterpret_cast<io_uring_cqe*>(cqrPtr_ + params_.cq_off.cqes);

    sqes_ = static_cast<io_uring_sqe*>(::mmap(nullptr, params_.sq_entries * sizeof(io_uring_sqe),
        PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ringFd_, IORING_OFF_SQES));
    if (sqes_ == MAP_FAILED) {
        cleanup();
        return false;
    }

    return true;
}

bool IoURing::isInitialized() const
{
    return ringFd_ != -1;
}

const io_uring_params& IoURing::getParams() const
{
    return params_;
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

std::optional<IoURing::CQEHandle> IoURing::peekCqeHandle(unsigned* numAvailable) const
{
    auto cqe = peekCqe(numAvailable);
    if (!cqe) {
        return std::nullopt;
    }
    return CQEHandle(this, cqe->user_data, cqe->res);
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
    if (num > 0) {
        const auto res = io_uring_enter(ringFd_, 0, num, IORING_ENTER_GETEVENTS);
        if (res < 0) {
            return nullptr;
        }
    }
    return peekCqe();
}

std::optional<IoURing::CQEHandle> IoURing::waitCqeHandle(size_t num) const
{
    auto cqe = waitCqe(num);
    if (!cqe) {
        return std::nullopt;
    }
    return CQEHandle(this, cqe->user_data, cqe->res);
}

void IoURing::advanceCq(size_t num) const
{
    assert(ringFd_ != -1);
    // Is this fence enough? (I am reading head again)
    *cqHeadPtr_ = *cqHeadPtr_ + num;
    writeBarrier();
}

size_t IoURing::getNumSqeEntries() const
{
    return sqEntries_;
}

size_t IoURing::getSqeCapacity() const
{
    readBarrier();
    return sqEntries_ - (sqesTail_ - *sqHeadPtr_);
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

io_uring_sqe* IoURing::prepare(uint8_t opcode, int fd, uint64_t off, const void* addr, uint32_t len)
{
    auto sqe = getSqe();
    if (!sqe) {
        return nullptr;
    }
    sqe->opcode = opcode;
    sqe->flags = 0;
    sqe->ioprio = 0;
    sqe->fd = fd;
    sqe->off = off;
    sqe->addr = reinterpret_cast<uint64_t>(addr);
    sqe->len = len;
    sqe->rw_flags = 0; // Init some union field with 0
    sqe->user_data = 0;
    sqe->__pad2[0] = sqe->__pad2[1] = sqe->__pad2[2] = 0;
    return sqe;
}

io_uring_sqe* IoURing::prepareNop()
{
    return prepare(IORING_OP_NOP, -1, 0, nullptr, 0);
}

io_uring_sqe* IoURing::prepareReadv(int fd, const iovec* iov, int iovcnt, off_t offset)
{
    return prepare(IORING_OP_READV, fd, offset, iov, iovcnt);
}

io_uring_sqe* IoURing::prepareWritev(int fd, const iovec* iov, int iovcnt, off_t offset)
{
    return prepare(IORING_OP_WRITEV, fd, offset, iov, iovcnt);
}

io_uring_sqe* IoURing::prepareFsync(int fd, uint32_t flags)
{
    auto sqe = prepare(IORING_OP_FSYNC, fd, 0, nullptr, 0);
    if (sqe) {
        sqe->fsync_flags = flags;
    }
    return sqe;
}

io_uring_sqe* IoURing::preparePollAdd(int fd, short events, uint32_t flags)
{
    auto sqe = prepare(IORING_OP_POLL_ADD, fd, 0, nullptr, flags);
    if (sqe) {
        sqe->poll_events = static_cast<unsigned short>(events);
    }
    return sqe;
}

io_uring_sqe* IoURing::preparePollRemove(uint64_t userData)
{
    return prepare(IORING_OP_POLL_REMOVE, -1, 0, reinterpret_cast<void*>(userData), 0);
}

io_uring_sqe* IoURing::prepareSyncFileRange(
    int fd, off64_t offset, off64_t nbytes, unsigned int flags)
{
    auto sqe = prepare(IORING_OP_SYNC_FILE_RANGE, fd, offset, nullptr, nbytes);
    if (sqe) {
        sqe->sync_range_flags = flags;
    }
    return sqe;
}

io_uring_sqe* IoURing::prepareSendmsg(int sockfd, const msghdr* msg, int flags)
{
    auto sqe = prepare(IORING_OP_SENDMSG, sockfd, 0, msg, 1);
    if (sqe) {
        sqe->msg_flags = flags;
    }
    return sqe;
}

io_uring_sqe* IoURing::prepareRecvmsg(int sockfd, const msghdr* msg, int flags)
{
    auto sqe = prepare(IORING_OP_RECVMSG, sockfd, 0, msg, 1);
    if (sqe) {
        sqe->msg_flags = flags;
    }
    return sqe;
}

io_uring_sqe* IoURing::prepareTimeout(struct __kernel_timespec* ts, uint64_t count, uint32_t flags)
{
    auto sqe = prepare(IORING_OP_TIMEOUT, -1, count, ts, 1);
    if (sqe) {
        sqe->timeout_flags = flags;
    }
    return sqe;
}

io_uring_sqe* IoURing::prepareTimeoutRemove(uint64_t userData, uint32_t flags)
{
    auto sqe = prepare(IORING_OP_TIMEOUT_REMOVE, -1, 0, reinterpret_cast<void*>(userData), 0);
    if (sqe) {
        sqe->timeout_flags = flags;
    }
    return sqe;
}

io_uring_sqe* IoURing::prepareAccept(int sockfd, sockaddr* addr, socklen_t* addrlen, uint32_t flags)
{
    auto sqe = prepare(IORING_OP_ACCEPT, sockfd, 0, addr, 0);
    if (sqe) {
        sqe->addr2 = reinterpret_cast<uint64_t>(addrlen);
        sqe->accept_flags = flags;
    }
    return sqe;
}

io_uring_sqe* IoURing::prepareAsyncCancel(uint64_t userData)
{
    return prepare(IORING_OP_ASYNC_CANCEL, -1, 0, reinterpret_cast<void*>(userData), 0);
}

io_uring_sqe* IoURing::prepareLinkTimeout(struct __kernel_timespec* ts)
{
    return prepare(IORING_OP_LINK_TIMEOUT, -1, 0, ts, 1);
}

io_uring_sqe* IoURing::prepareConnect(int sockfd, const sockaddr* addr, socklen_t addrlen)
{
    return prepare(IORING_OP_CONNECT, sockfd, addrlen, addr, 0);
}

io_uring_sqe* IoURing::prepareOpenat(int dirfd, const char* pathname, int flags, mode_t mode)
{
    auto sqe = prepare(IORING_OP_OPENAT, dirfd, 0, pathname, mode);
    if (sqe) {
        sqe->open_flags = flags;
    }
    return sqe;
}

io_uring_sqe* IoURing::prepareClose(int fd)
{
    return prepare(IORING_OP_CLOSE, fd, 0, nullptr, 0);
}

io_uring_sqe* IoURing::prepareStatx(
    int dirfd, const char* pathname, int flags, unsigned int mask, struct statx* statxbuf)
{
    auto sqe
        = prepare(IORING_OP_STATX, dirfd, reinterpret_cast<uint64_t>(statxbuf), pathname, mask);
    if (sqe) {
        sqe->statx_flags = flags;
    }
    return sqe;
}

io_uring_sqe* IoURing::prepareRead(int fd, void* buf, size_t count, off_t offset)
{
    return prepare(IORING_OP_READ, fd, offset, buf, count);
}

io_uring_sqe* IoURing::prepareWrite(int fd, const void* buf, size_t count, off_t offset)
{
    return prepare(IORING_OP_WRITE, fd, offset, buf, count);
}

io_uring_sqe* IoURing::prepareSend(int sockfd, const void* buf, size_t len, int flags)
{
    auto sqe = prepare(IORING_OP_SEND, sockfd, 0, buf, len);
    if (sqe) {
        sqe->msg_flags = flags;
    }
    return sqe;
}

io_uring_sqe* IoURing::prepareRecv(int sockfd, void* buf, size_t len, int flags)
{
    auto sqe = prepare(IORING_OP_RECV, sockfd, 0, buf, len);
    if (sqe) {
        sqe->msg_flags = flags;
    }
    return sqe;
}

io_uring_sqe* IoURing::prepareOpenat2(int dirfd, const char* pathname, const open_how* how)
{
    return prepare(
        IORING_OP_OPENAT2, dirfd, reinterpret_cast<uint64_t>(how), pathname, sizeof(open_how));
}

io_uring_sqe* IoURing::prepareEpollCtl(int epfd, int op, int fd, epoll_event* event)
{
    return prepare(IORING_OP_EPOLL_CTL, epfd, reinterpret_cast<uint64_t>(event),
        reinterpret_cast<void*>(fd), op);
}

io_uring_sqe* IoURing::prepareShutdown(int fd, int how)
{
    return prepare(IORING_OP_SHUTDOWN, fd, 0, nullptr, how);
}

io_uring_sqe* IoURing::prepareRenameat(
    int olddirfd, const char* oldpath, int newdirfd, const char* newpath, int flags)
{
    auto sqe = prepare(
        IORING_OP_RENAMEAT, olddirfd, reinterpret_cast<uint64_t>(newpath), oldpath, newdirfd);
    if (sqe) {
        sqe->rename_flags = flags;
    }
    return sqe;
}

io_uring_sqe* IoURing::prepareUnlinkat(int dirfd, const char* pathname, int flags)
{
    auto sqe = prepare(IORING_OP_UNLINKAT, dirfd, 0, pathname, 0);
    if (sqe) {
        sqe->unlink_flags = flags;
    }
    return sqe;
}
