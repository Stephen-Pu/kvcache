// Shared hashing utilities (BLAKE3-128, FNV-1a placeholders).
// TODO(stephen): integrate the BLAKE3 reference implementation.
#include <cstddef>  // std::size_t — not transitively provided by libc++
#include <cstdint>

namespace kvcache {

// Placeholder — real implementation will use BLAKE3 from a vendored dep.
uint64_t Fnv1a64(const void* data, std::size_t n) {
    const auto* p = static_cast<const uint8_t*>(data);
    uint64_t h = 0xcbf29ce484222325ULL;
    for (std::size_t i = 0; i < n; ++i) {
        h ^= p[i];
        h *= 0x100000001b3ULL;
    }
    return h;
}

}  // namespace kvcache
