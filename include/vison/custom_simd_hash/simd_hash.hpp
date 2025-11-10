#pragma once

// ---- handy 64-bit mix helpers ----
static inline uint64_t rotl64(uint64_t x, unsigned r) {
    return (x << r) | (x >> (64 - r));
}

static inline uint64_t mix64(uint64_t x) { //eli
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

// Read up to 8 bytes safely
static inline uint64_t rd64_tail(const unsigned char* p, size_t n) {
    // n is 1..8
    uint64_t v = 0;
    std::memcpy(&v, p, n);
    return v;
}

// -----------------------------------------------------------------------------
// Scalar 64-bit streaming hash
// Processes input in 8-byte chunks using a strong mixing function.
// This design balances speed and entropy, suitable for hash tables
// (non-cryptographic, but high-quality and collision-resistant).
// -----------------------------------------------------------------------------
static inline uint64_t hash_scalar(std::string_view s, uint64_t seed = 0) {
    const unsigned char* p = reinterpret_cast<const unsigned char*>(s.data());
    size_t len = s.size();

    if (len <= 16) {
        uint64_t v = rd64_tail(p, len);
        return mix64(v ^ seed ^ 0x9e3779b97f4a7c15ULL);
    }

    // Initialize hash state using input length and a golden-ratio constant.
    // This ensures that different-length strings with the same prefix
    // produce distinct starting states.
    uint64_t h = seed ^ (len * 0x9e3779b97f4a7c15ULL);

    // Process the string in 8-byte (64-bit) blocks for maximum throughput.
    while (len >= 8) {
        uint64_t v;
        std::memcpy(&v, p, 8);                     // Safe, alignment-agnostic load
        v ^= 0x9e3779b97f4a7c15ULL;                // Pre-whitening: break low-entropy patterns, that is the golden ratio
        h ^= mix64(v);                             // Mix block entropy into hash state
        h = rotl64(h, 27) * 0x94d049bb133111ebULL; // Rotate and multiply to decorrelate order
        p += 8;
        len -= 8;
    }

    // Process remaining tail bytes (1–7) if any.
    // Tail is mixed with a different constant to ensure unique contribution.
    if (len) {
        uint64_t t = rd64_tail(p, len);            // Read remaining bytes safely
        t ^= 0x27d4eb2f165667c5ULL;                // Distinguish tail pattern
        h ^= mix64(t);                             // Integrate tail into state
    }

    // Final avalanche: ensures every input bit affects every output bit.
    return mix64(h);
}

#if defined(__AVX2__)
// AVX2 32 bytes step hasher
static inline uint64_t hash_avx2(std::string_view s, uint64_t seed = 0) {
    const unsigned char* p = reinterpret_cast<const unsigned char*>(s.data());
    size_t len = s.size();

    if (len <= 16) {
        uint64_t v = rd64_tail(p, len);
        return mix64(v ^ seed ^ 0x9e3779b97f4a7c15ULL);
    }

    // Initialize 4 parallel lane states with different salts to avoid collisions
    __m256i state = _mm256_set_epi64x(
        seed ^ 0x9e3779b97f4a7c15ULL,
        seed ^ 0xc2b2ae3d27d4eb4fULL,
        seed ^ 0x165667b19e3779f9ULL,
        seed ^ 0x85ebca77c2b2ae63ULL
    );

    const __m256i prime1 = _mm256_set1_epi64x(0x9ddfea08eb382d69ULL);
    const __m256i prime2 = _mm256_set1_epi64x(0xc2b2ae3d27d4eb4fULL);

    while (len >= 32) {
        __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p)); // 32bytes
        // Mix: xor, rotate via shuffle+shift, multiply by big primes, xor accumulate

        //here each 4 lanes get one 8 bytes chunk
        __m256i x = _mm256_xor_si256(v, state);

        // "rotate" 64-bit lanes (approx) by mixing with shifts
        __m256i xl = _mm256_slli_epi64(x, 31);
        __m256i xr = _mm256_srli_epi64(x, 33);
        __m256i r  = _mm256_or_si256(xl, xr);

        // simulate 64-bit wide multiply mix: mul 32-bit halves and xor (fast + portable)
        __m256i m1 = _mm256_mul_epu32(r, prime1);
        __m256i m2 = _mm256_mul_epu32(_mm256_srli_epi64(r, 32), prime2);
        state = _mm256_xor_si256(m1, m2);

        p += 32; 
        len -= 32;
    }

    // collapse 4x64 into single 64
    uint64_t lanes[4];
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(lanes), state);
    uint64_t h = lanes[0] ^ lanes[1] ^ lanes[2] ^ lanes[3] ^ (s.size() * 0x9e3779b97f4a7c15ULL);

    // Tail ≥ 8B chunks
    while (len >= 8) {
        uint64_t v;
        std::memcpy(&v, p, 8);
        v ^= 0x2545F4914F6CDD1DULL;
        h ^= mix64(v);
        h = rotl64(h, 29) * 0x94d049bb133111ebULL;
        p += 8; len -= 8;
    }
    if (len) {
        uint64_t t = rd64_tail(p, len);
        t ^= 0x9e3779b185ebca87ULL;
        h ^= mix64(t);
    }
    return mix64(h);
}
#endif

struct simd_hash {
    using is_avalanching = void;

    uint64_t operator()(std::string_view s) const noexcept {
    #if defined(__AVX2__)
        return hash_avx2(s);
    #else
        return hash_scalar(s);
    #endif
    }
};



