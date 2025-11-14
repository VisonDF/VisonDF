#if defined(__AVX512F__)

inline void histogram_pass_u32_avx512_16buckets(
    const uint32_t* __restrict tkeys,
    size_t n,
    size_t shift,
    size_t* __restrict count   // K = 65536 buckets
)
{
    const __m512i mask = _mm512_set1_epi32(0xFFFF);

    // 16 independent histograms: [lane][bucket]
    size_t* __restrict local = get_local_histogram_16x_u32();
    memset(local, 0, RADIX_LANES_AVX512 * RADIX_KI32 * sizeof(size_t));

    // Precompute lane pointers: lanes[lane][bucket]
    size_t* lanes[RADIX_LANES_AVX512];
    for (size_t lane = 0; lane < RADIX_LANES_AVX512; ++lane) {
        lanes[lane] = local + lane * RADIX_KI32;
    }

    size_t i = 0;

    // SIMD main loop: process 16 keys per iteration
    for (; i + 16 <= n; i += 16) {
        // optional prefetch ahead
        __builtin_prefetch(tkeys + i + 128, 0, 1);

        __m512i v = _mm512_loadu_si512(
            reinterpret_cast<const void*>(tkeys + i)
        );

        // shift and mask → 16 bucket indices in [0, 65535]
        __m512i buckets = _mm512_and_si512(
            _mm512_srli_epi32(v, shift),
            mask
        );

        // store lanes to stack
        alignas(64) uint32_t b[16];
        _mm512_storeu_si512(reinterpret_cast<void*>(b), buckets);

        // increment 16 lane histograms
        #pragma unroll(16)
        for (int lane = 0; lane < 16; ++lane) {
            lanes[lane][b[lane]]++;
        }
    }

    // Scalar tail → just use lane 0
    for (; i < n; ++i) {
        const uint32_t bucket = (tkeys[i] >> shift) & 0xFFFFu;
        lanes[0][bucket]++;
    }

    // Reduce 16 lane histograms into final count[]
    for (size_t b = 0; b < RADIX_KI32; ++b) {
        size_t s = 0;
        // unroll manually for max throughput
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


