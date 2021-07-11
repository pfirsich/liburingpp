#pragma once

#include <cstdint>
#include <optional>

#include <linux/io_uring.h>
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

    io_uring_cqe* peekCqe(unsigned* numAvailable = nullptr) const;
    std::optional<CQEHandle> peekCqeHandle(unsigned* numAvailable = nullptr) const;
    io_uring_cqe* waitCqe(size_t num = 1) const;
    std::optional<CQEHandle> waitCqeHandle(size_t num = 1) const;
    void advanceCq(size_t num = 1) const;

    io_uring_sqe* getSqe();
    size_t flushSqes(size_t num = 0);
    void submitSqes(size_t num = 0, size_t waitCqes = 0);

    io_uring_sqe* prepareReadv(int fd, const iovec* iov, int iovcnt, off_t offset = 0);

private:
    void cleanup();
    void release();

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
