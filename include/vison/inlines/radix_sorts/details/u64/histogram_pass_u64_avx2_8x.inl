#pragma once

#if defined(__AVX2__)

inline void histogram_pass_u64_avx2_8buckets(
    const uint64_t* __restrict tkeys,
    size_t n,
    size_t shift,
    size_t* __restrict count   // 65536 buckets
)
{
    const __m256i mask = _mm256_set1_epi64x(0xFFFFull);

    // 8 independent histograms: [lane][bucket]
    size_t* __restrict local = get_local_histogram_8x_u16();
    std::memset(local, 0, RADIX_LANES * RADIX_KI16 * sizeof(size_t));

    // Lane pointers
    size_t* lane0 = local + 0 * RADIX_KI16;
    size_t* lane1 = local + 1 * RADIX_KI16;
    size_t* lane2 = local + 2 * RADIX_KI16;
    size_t* lane3 = local + 3 * RADIX_KI16;
    size_t* lane4 = local + 4 * RADIX_KI16;
    size_t* lane5 = local + 5 * RADIX_KI16;
    size_t* lane6 = local + 6 * RADIX_KI16;
    size_t* lane7 = local + 7 * RADIX_KI16;

    size_t i = 0;

    // Process 8 keys per iteration → 1 key per lane
    for (; i + 8 <= n; i += 8)
    {
        __builtin_prefetch(tkeys + i + 64, 0, 1);

        // Load 8 × u64 as 2 × 256-bit vectors
        __m256i v0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(tkeys + i + 0)); // 0..3
        __m256i v1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(tkeys + i + 4)); // 4..7

        // Shift and mask → bucket indices in [0, 65535]
        __m256i s0 = _mm256_srli_epi64(v0, shift);
        __m256i s1 = _mm256_srli_epi64(v1, shift);

        __m256i b0 = _mm256_and_si256(s0, mask);
        __m256i b1 = _mm256_and_si256(s1, mask);

        // Spill to stack
        alignas(32) uint64_t b[8];
        _mm256_store_si256(reinterpret_cast<__m256i*>(b + 0), b0);
        _mm256_store_si256(reinterpret_cast<__m256i*>(b + 4), b1);

        // 1 element per lane
        lane0[b[0]]++;
        lane1[b[1]]++;
        lane2[b[2]]++;
        lane3[b[3]]++;
        lane4[b[4]]++;
        lane5[b[5]]++;
        lane6[b[6]]++;
        lane7[b[7]]++;
    }

    // Scalar tail → lane0
    for (; i < n; ++i) {
        uint64_t bucket = (tkeys[i] >> shift) & 0xFFFFu;
        lane0[bucket]++;
    }

    // Reduce 8 lanes into final count[]
    for (size_t b = 0; b < RADIX_KI16; ++b) {
        count[b] =
            lane0[b] + lane1[b] + lane2[b] + lane3[b] +
            lane4[b] + lane5[b] + lane6[b] + lane7[b];
    }
}

#endif


