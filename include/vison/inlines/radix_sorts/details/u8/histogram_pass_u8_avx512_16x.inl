#if defined(__AVX512F__)

inline void histogram_pass_u8_avx512_16buckets(
    const uint8_t* __restrict data,
    size_t n,
    size_t* __restrict count   // K = 256 buckets
)
{
    // 16 local histograms of 256 entries each
    size_t* __restrict local = get_local_histogram_16x_u8();
    memset(local, 0, LANES_AVX512 * RADIX_KI8 * sizeof(size_t));

    // lane pointers
    size_t* lanes[LANES_AVX512];
    for (size_t lane = 0; lane < LANES_AVX512; ++lane) {
        lanes[lane] = local + lane * RADIX_KI8;
    }

    size_t i = 0;

    // SIMD main loop: 16 bytes per iteration
    for (; i + 16 <= n; i += 16) {

        __builtin_prefetch(data + i + 128, 0, 1);

        // load 16 uint8 → they are zero-extended into 32-bit slots
        __m512i v = _mm512_cvtepu8_epi32(
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(data + i))
        );

        // store to stack
        alignas(64) uint32_t b[16];
        _mm512_storeu_si512(b, v);

        // 16 independent lane increments
        #pragma unroll(16)
        for (int lane = 0; lane < 16; ++lane) {
            lanes[lane][b[lane]]++;
        }
    }

    // tail → lane 0
    for (; i < n; ++i) {
        lanes[0][data[i]]++;
    }

    // reduce 16 histograms
    for (size_t b = 0; b < K; ++b) {
        size_t s = 0;
        s += lanes[0][b];   s += lanes[1][b];
        s += lanes[2][b];   s += lanes[3][b];
        s += lanes[4][b];   s += lanes[5][b];
        s += lanes[6][b];   s += lanes[7][b];
        s += lanes[8][b];   s += lanes[9][b];
        s += lanes[10][b];  s += lanes[11][b];
        s += lanes[12][b];  s += lanes[13][b];
        s += lanes[14][b];  s += lanes[15][b];

        count[b] = s;
    }
}

#endif


