#pragma once

#if defined(__AVX512F__)

inline void histogram_pass_u64_avx512_16buckets(
    const uint64_t* __restrict tkeys,
    size_t n,
    size_t shift,
    size_t* __restrict count   // 65536 buckets
)
{
    const __m512i mask = _mm512_set1_epi64(0xFFFFull);

    // 16 independent histograms: [lane][bucket]
    size_t* __restrict local = get_local_histogram_16x_u64();
    std::memset(local, 0, RADIX_LANES_AVX512 * RADIX_KI64 * sizeof(size_t));

    // lane[bucket] pointers
    size_t* lanes[RADIX_LANES_AVX512];
    for (size_t lane = 0; lane < RADIX_LANES_AVX512; ++lane) {
        lanes[lane] = local + lane * RADIX_KI64;
    }

    size_t i = 0;

    // SIMD main loop: process 16 keys per iteration (2 × 8×u64)
    for (; i + 16 <= n; i += 16)
    {
        __builtin_prefetch(tkeys + i + 128, 0, 1);

        // Load 16×u64 as two 512-bit vectors
        __m512i v0 = _mm512_loadu_si512(reinterpret_cast<const void*>(tkeys + i + 0));  // 0..7
        __m512i v1 = _mm512_loadu_si512(reinterpret_cast<const void*>(tkeys + i + 8));  // 8..15

        // Shift and mask → 16 bucket indices in [0, 65535]
        __m512i s0 = _mm512_srli_epi64(v0, shift);
        __m512i s1 = _mm512_srli_epi64(v1, shift);

        __m512i b0 = _mm512_and_si512(s0, mask);
        __m512i b1 = _mm512_and_si512(s1, mask);

        // Store to stack
        alignas(64) uint64_t b[16];
        _mm512_store_si512(reinterpret_cast<__m512i*>(b +  0), b0);
        _mm512_store_si512(reinterpret_cast<__m512i*>(b +  8), b1);

        // Increment 16 independent lane histograms
        #pragma unroll(16)
        for (int lane = 0; lane < 16; ++lane) {
            lanes[lane][ b[lane] ]++;
        }
    }

    // Scalar tail → just use lane 0
    for (; i < n; ++i) {
        uint64_t bucket = (tkeys[i] >> shift) & 0xFFFFull;
        lanes[0][bucket]++;
    }

    // Reduce 16 lane histograms into final count[]
    for (size_t b = 0; b < RADIX_KI64; ++b) {
        size_t s = 0;
        // unrolled for throughput
        s += lanes[0][b];
        s += lanes[1][b];
        s += lanes[2][b];
        s += lanes[3][b];
        s += lanes[4][b];
        s += lanes[5][b];
        s += lanes[6][b];
        s += lanes[7][b];
        s += lanes[8][b];
        s += lanes[9][b];
        s += lanes[10][b];
        s += lanes[11][b];
        s += lanes[12][b];
        s += lanes[13][b];
        s += lanes[14][b];
        s += lanes[15][b];

        count[b] = s;
    }
}

#endif


