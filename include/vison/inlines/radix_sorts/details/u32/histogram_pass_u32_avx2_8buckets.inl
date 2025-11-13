#pragma once

#if defined(__AVX2__)

inline void histogram_pass_u32_avx2_8buckets(
    const uint32_t* __restrict tkeys,
    size_t n,
    size_t shift,
    size_t* __restrict count
)
{
    const __m256i mask = _mm256_set1_epi32(0xFFFF);

    size_t* __restrict local = get_local_histogram_8x();
    memset(local, 0, RADIX_LANES * RADIX_KI32 * sizeof(size_t));

    size_t* lanes[RADIX_LANES] = {
        local + 0 * RADIX_KI32,
        local + 1 * RADIX_KI32,
        local + 2 * RADIX_KI32,
        local + 3 * RADIX_KI32,
        local + 4 * RADIX_KI32,
        local + 5 * RADIX_KI32,
        local + 6 * RADIX_KI32,
        local + 7 * RADIX_KI32
    };

    size_t i = 0;

    for (; i + 8 <= n; i += 8)
    {
        __builtin_prefetch(tkeys + i + 64, 0, 1);

        __m256i v = _mm256_loadu_si256((__m256i const*)(tkeys + i));
        __m256i buckets = _mm256_and_si256(_mm256_srli_epi32(v, shift), mask);

        alignas(32) uint32_t b[8];
        _mm256_storeu_si256((__m256i*)b, buckets);

        #pragma unroll(8)
        for (int lane = 0; lane < 8; lane++)
            lanes[lane][b[lane]]++;
    }

    // Scalar tail
    for (; i < n; i++) {
        uint32_t bucket = (tkeys[i] >> shift) & 0xFFFFu;
        lanes[0][bucket]++;
    }

    // Reduction
    for (size_t b = 0; b < RADIX_KI32; b++) {
        size_t s =
            lanes[0][b] +
            lanes[1][b] +
            lanes[2][b] +
            lanes[3][b] +
            lanes[4][b] +
            lanes[5][b] +
            lanes[6][b] +
            lanes[7][b];

        count[b] = s;
    }
}

#endif


