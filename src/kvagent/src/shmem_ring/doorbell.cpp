// LLD §6.1.3 — eventfd (Linux) / pipe-fallback (macOS) doorbell.
#include "shmem_ring/doorbell.h"

#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstring>

#if __has_include(<sys/eventfd.h>)
#  include <sys/eventfd.h>
#  define KVCACHE_HAS_EVENTFD 1
#else
#  define KVCACHE_HAS_EVENTFD 0
#endif

namespace kvcache::agent::shmem_ring {

static void CloseFd(int fd) noexcept {
    if (fd >= 0) {
        while (::close(fd) == -1 && errno == EINTR) {}
    }
}

std::unique_ptr<Doorbell> Doorbell::Create(std::string* err) {
    auto self = std::unique_ptr<Doorbell>(new Doorbell());
#if KVCACHE_HAS_EVENTFD
    // EFD_NONBLOCK so Ring() never blocks (we tolerate counter
    // saturation); EFD_CLOEXEC so fork() doesn't leak the fd.
    int fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (fd < 0) {
        if (err) *err = std::string("eventfd: ") + std::strerror(errno);
        return nullptr;
    }
    self->read_fd_  = fd;
    self->write_fd_ = fd;
    self->is_pipe_  = false;
#else
    int fds[2];
    if (::pipe(fds) != 0) {
        if (err) *err = std::string("pipe: ") + std::strerror(errno);
        return nullptr;
    }
    for (int i = 0; i < 2; ++i) {
        int flags = ::fcntl(fds[i], F_GETFL, 0);
        if (flags < 0 || ::fcntl(fds[i], F_SETFL, flags | O_NONBLOCK) < 0) {
            if (err) *err = std::string("fcntl(NONBLOCK): ") + std::strerror(errno);
            CloseFd(fds[0]); CloseFd(fds[1]);
            return nullptr;
        }
        flags = ::fcntl(fds[i], F_GETFD, 0);
        if (flags < 0 || ::fcntl(fds[i], F_SETFD, flags | FD_CLOEXEC) < 0) {
            if (err) *err = std::string("fcntl(CLOEXEC): ") + std::strerror(errno);
            CloseFd(fds[0]); CloseFd(fds[1]);
            return nullptr;
        }
    }
    self->read_fd_  = fds[0];
    self->write_fd_ = fds[1];
    self->is_pipe_  = true;
#endif
    return self;
}

Doorbell::~Doorbell() {
    if (is_pipe_) {
        CloseFd(read_fd_);
        CloseFd(write_fd_);
    } else {
        CloseFd(read_fd_);
    }
}

bool Doorbell::Ring() noexcept {
#if KVCACHE_HAS_EVENTFD
    const uint64_t one = 1;
    ssize_t n = ::write(write_fd_, &one, sizeof(one));
    if (n == sizeof(one)) return true;
    // EAGAIN = counter saturated at UINT64_MAX-1; consumer will see a
    // wakeup on its next Wait, so treat as OK.
    return errno == EAGAIN;
#else
    const uint8_t one = 1;
    ssize_t n = ::write(write_fd_, &one, sizeof(one));
    if (n == 1) return true;
    return errno == EAGAIN;
#endif
}

int Doorbell::Wait(int timeout_ms) noexcept {
    struct ::pollfd p{};
    p.fd     = read_fd_;
    p.events = POLLIN;
    int r;
    do {
        r = ::poll(&p, 1, timeout_ms);
    } while (r < 0 && errno == EINTR);
    if (r < 0) return -1;
    if (r == 0) return 0;
#if KVCACHE_HAS_EVENTFD
    uint64_t buf;
    while (::read(read_fd_, &buf, sizeof(buf)) > 0) {}
#else
    uint8_t buf[64];
    while (::read(read_fd_, buf, sizeof(buf)) > 0) {}
#endif
    return 1;
}

}  // namespace kvcache::agent::shmem_ring
