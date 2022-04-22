#pragma once

#include <cstdint>
#include <optional>

#include <linux/io_uring.h>
#include <linux/openat2.h>
#include <linux/time_types.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/uio.h>

// Heavily inspired by liburing

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
    struct CQEHandle {
        const IoURing* ring = nullptr;
        uint64_t userData;
        int32_t res;

        CQEHandle(const IoURing* ring, uint64_t userData, int32_t res);

        CQEHandle(const CQEHandle&) = delete;
        CQEHandle& operator=(const CQEHandle&) = delete;

        CQEHandle(CQEHandle&& other);
        CQEHandle& operator=(CQEHandle&& other);

        ~CQEHandle();

        void finish();
        void release();
    };

    IoURing() = default;
    ~IoURing();

    // Delete copy construction/assignment,
    // because we want single ownership of the underlying ring (esp. the fd).
    IoURing(const IoURing&) = delete;
    IoURing& operator=(const IoURing&) = delete;

    // TODO: Implement these two
    IoURing(IoURing&&) = delete;
    IoURing& operator=(IoURing&&) = delete;

    bool init(size_t sqEntries = 128);
    bool isInitialized() const;

    const io_uring_params& getParams() const;

    io_uring_cqe* peekCqe(unsigned* numAvailable = nullptr) const;
    std::optional<CQEHandle> peekCqeHandle(unsigned* numAvailable = nullptr) const;
    io_uring_cqe* waitCqe(size_t num = 1) const;
    std::optional<CQEHandle> waitCqeHandle(size_t num = 1) const;
    void advanceCq(size_t num = 1) const;

    size_t getNumSqeEntries() const;
    size_t getSqeCapacity() const;
    io_uring_sqe* getSqe();
    size_t flushSqes(size_t num = 0);
    void submitSqes(size_t num = 0, size_t waitCqes = 0);

    io_uring_sqe* prepare(uint8_t opcode, int fd, uint64_t off, const void* addr, uint32_t len);
    io_uring_sqe* prepareNop();
    io_uring_sqe* prepareReadv(int fd, const iovec* iov, int iovcnt, off_t offset = 0);
    io_uring_sqe* prepareWritev(int fd, const iovec* iov, int iovcnt, off_t offset = 0);
    io_uring_sqe* prepareFsync(int fd, uint32_t flags = 0);
    // io_uring_sqe* prepareReadFixed();
    // io_uring_sqe* prepareWriteFixed();
    io_uring_sqe* preparePollAdd(int fd, short events, uint32_t flags = 0);
    io_uring_sqe* preparePollRemove(uint64_t userData);
    io_uring_sqe* prepareSyncFileRange(
        int fd, off64_t offset, off64_t nbytes, unsigned int flags = 0);
    io_uring_sqe* prepareSendmsg(int sockfd, const msghdr* msg, int flags = 0);
    io_uring_sqe* prepareRecvmsg(int sockfd, const msghdr* msg, int flags = 0);
    io_uring_sqe* prepareTimeout(__kernel_timespec* ts, uint64_t count, uint32_t flags = 0);
    io_uring_sqe* prepareTimeoutRemove(uint64_t userData, uint32_t flags);
    io_uring_sqe* prepareAccept(int sockfd, sockaddr* addr, socklen_t* addrlen, uint32_t flags = 0);
    io_uring_sqe* prepareAsyncCancel(uint64_t userData);
    io_uring_sqe* prepareLinkTimeout(__kernel_timespec* ts);
    io_uring_sqe* prepareConnect(int sockfd, const sockaddr* addr, socklen_t addrlen);
    // io_uring_sqe* prepareFallocate();
    io_uring_sqe* prepareOpenat(int dirfd, const char* pathname, int flags, mode_t mode);
    io_uring_sqe* prepareClose(int fd);
    // io_uring_sqe* prepareFilesUpdate();
    io_uring_sqe* prepareStatx(
        int dirfd, const char* pathname, int flags, unsigned int mask, struct statx* statxbuf);
    io_uring_sqe* prepareRead(int fd, void* buf, size_t count, off_t offset = 0);
    io_uring_sqe* prepareWrite(int fd, const void* buf, size_t count, off_t offset = 0);
    // io_uring_sqe* prepareFadvise();
    // io_uring_sqe* prepareMadvise();
    io_uring_sqe* prepareSend(int sockfd, const void* buf, size_t len, int flags = 0);
    io_uring_sqe* prepareRecv(int sockfd, void* buf, size_t len, int flags = 0);
    io_uring_sqe* prepareOpenat2(int dirfd, const char* pathname, const open_how* how);
    io_uring_sqe* prepareEpollCtl(int epfd, int op, int fd, epoll_event* event);
    // io_uring_sqe* prepareSplice(int fd_in, loff_t* off_in, int fd_out, loff_t* off_out,
    //                             size_t len, unsigned int flags);
    // io_uring_sqe* prepareProvideBuffers();
    // io_uring_sqe* prepareRemoveBuffers();
    // io_uring_sqe* prepareTee(int fdIn, int fdOut, size_t len, unsigned int flags);
    io_uring_sqe* prepareShutdown(int fd, int how);
    io_uring_sqe* prepareRenameat(
        int olddirfd, const char* oldpath, int newdirfd, const char* newpath, int flags = 0);
    io_uring_sqe* prepareUnlinkat(int dirfd, const char* pathname, int flags = 0);

private:
    void cleanup();
    void release();

    io_uring_params params_;

    int ringFd_ = -1;

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

    // These two variables are the only member variables that change over the lifetime
    // of this object.
    // All others above are only set once by init().
    // These two are the head/tail of the sqes_ array (containing actual io_uring_sqe),
    // while sqHeadPtr_ and sqTailPtr_ are the head/tail of the sqIndexArray_ (with unsigneds).
    size_t sqesHead_ = 0;
    size_t sqesTail_ = 0;
};
