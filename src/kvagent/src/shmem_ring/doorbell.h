// LLD §6.1.3 — eventfd-based doorbell to avoid burning CPU in pop_for.
//
// Phase A1.2 — wake-up channel next to a SqCq ring. The producer
// rings the bell after a push (a single ``Ring()`` the kernel
// coalesces into a counter); the consumer Wait()s on the fd for an
// edge.
//
// eventfd is process-shared natively, which is what we need
// (engine → agent). macOS has no eventfd → fall back to a pipe pair.
// Public surface is identical on both platforms.
#pragma once

#include <memory>
#include <string>

namespace kvcache::agent::shmem_ring {

class Doorbell {
   public:
    static std::unique_ptr<Doorbell> Create(std::string* err);

    ~Doorbell();
    Doorbell(const Doorbell&)            = delete;
    Doorbell& operator=(const Doorbell&) = delete;

    // Wake any waiting consumer. Coalescing — multiple Ring()s before
    // a single Wait() return together. Returns false only on a real
    // fd error (closed under us).
    bool Ring() noexcept;

    // Block until rung or timeout. Returns:
    //   1 — bell rang (drained)
    //   0 — timed out
    //  -1 — error (errno set; EINTR caller can retry)
    int Wait(int timeout_ms) noexcept;

    // Exposed so the kvagent event loop can multiplex into epoll/select.
    int fd() const noexcept { return read_fd_; }

   private:
    Doorbell() = default;
    int  read_fd_  = -1;
    int  write_fd_ = -1;  // same as read_fd_ on Linux eventfd
    bool is_pipe_  = false;
};

}  // namespace kvcache::agent::shmem_ring
