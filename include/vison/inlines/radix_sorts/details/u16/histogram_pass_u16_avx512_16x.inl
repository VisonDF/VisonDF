#pragma once

#if defined(__AVX512F__)

inline void histogram_pass_u16_avx512_16buckets(
    const uint16_t* __restrict data,
    size_t n,
    size_t* __restrict count   // RADIX_KI16 = 65536
)
{
    // 16 independent histograms: [lane][bucket]
    size_t* __restrict local = get_local_histogram_16x_u16();
    memset(local, 0, RADIX_LANES_AVX512 * RADIX_KI16 * sizeof(size_t));

    // lane pointers
    size_t* lane0  = local +  0 * RADIX_KI16;
    size_t* lane1  = local +  1 * RADIX_KI16;
    size_t* lane2  = local +  2 * RADIX_KI16;
    size_t* lane3  = local +  3 * RADIX_KI16;
    size_t* lane4  = local +  4 * RADIX_KI16;
    size_t* lane5  = local +  5 * RADIX_KI16;
    size_t* lane6  = local +  6 * RADIX_KI16;
    size_t* lane7  = local +  7 * RADIX_KI16;
    size_t* lane8  = local +  8 * RADIX_KI16;
    size_t* lane9  = local +  9 * RADIX_KI16;
    size_t* lane10 = local + 10 * RADIX_KI16;
    size_t* lane11 = local + 11 * RADIX_KI16;
    size_t* lane12 = local + 12 * RADIX_KI16;
    size_t* lane13 = local + 13 * RADIX_KI16;
    size_t* lane14 = local + 14 * RADIX_KI16;
    size_t* lane15 = local + 15 * RADIX_KI16;

    size_t i = 0;

    // Main loop: 64 x u16 per iteration
    //  => 4 elements per lane × 16 lanes
    for (; i + 64 <= n; i += 64)
    {
        __builtin_prefetch(data + i + 256, 0, 1);

        __m512i v1 = _mm512_loadu_si512((const void*)(data + i));
        __m512i v2 = _mm512_loadu_si512((const void*)(data + i + 32));

        alignas(64) uint16_t b1[32];
        alignas(64) uint16_t b2[32];

        _mm512_store_si512((void*)b1, v1);
        _mm512_store_si512((void*)b2, v2);

        // 4 increments per lane: 2 from b1, 2 from b2

        lane0 [b1[ 0]]++; lane0 [b1[ 1]]++;
        lane0 [b2[ 0]]++; lane0 [b2[ 1]]++;

        lane1 [b1[ 2]]++; lane1 [b1[ 3]]++;
        lane1 [b2[ 2]]++; lane1 [b2[ 3]]++;

        lane2 [b1[ 4]]++; lane2 [b1[ 5]]++;
        lane2 [b2[ 4]]++; lane2 [b2[ 5]]++;

        lane3 [b1[ 6]]++; lane3 [b1[ 7]]++;
        lane3 [b2[ 6]]++; lane3 [b2[ 7]]++;

        lane4 [b1[ 8]]++; lane4 [b1[ 9]]++;
        lane4 [b2[ 8]]++; lane4 [b2[ 9]]++;

        lane5 [b1[10]]++; lane5 [b1[11]]++;
        lane5 [b2[10]]++; lane5 [b2[11]]++;

        lane6 [b1[12]]++; lane6 [b1[13]]++;
        lane6 [b2[12]]++; lane6 [b2[13]]++;

        lane7 [b1[14]]++; lane7 [b1[15]]++;
        lane7 [b2[14]]++; lane7 [b2[15]]++;

        lane8 [b1[16]]++; lane8 [b1[17]]++;
        lane8 [b2[16]]++; lane8 [b2[17]]++;

        lane9 [b1[18]]++; lane9 [b1[19]]++;
        lane9 [b2[18]]++; lane9 [b2[19]]++;

        lane10[b1[20]]++; lane10[b1[21]]++;
        lane10[b2[20]]++; lane10[b2[21]]++;

        lane11[b1[22]]++; lane11[b1[23]]++;
        lane11[b2[22]]++; lane11[b2[23]]++;

        lane12[b1[24]]++; lane12[b1[25]]++;
        lane12[b2[24]]++; lane12[b2[25]]++;

        lane13[b1[26]]++; lane13[b1[27]]++;
        lane13[b2[26]]++; lane13[b2[27]]++;

        lane14[b1[28]]++; lane14[b1[29]]++;
        lane14[b2[28]]++; lane14[b2[29]]++;

        lane15[b1[30]]++; lane15[b1[31]]++;
        lane15[b2[30]]++; lane15[b2[31]]++;
    }

    // avx512 tail
    for (; i + 32 <= n; i += 32)
    {
        __m512i v = _mm512_loadu_si512((const void*)(data + i));

        alignas(64) uint16_t b[32];
        _mm512_store_si512((void*)b, v);

        lane0 [b[ 0]]++; lane0 [b[ 1]]++;
        lane1 [b[ 2]]++; lane1 [b[ 3]]++;
        lane2 [b[ 4]]++; lane2 [b[ 5]]++;
        lane3 [b[ 6]]++; lane3 [b[ 7]]++;
        lane4 [b[ 8]]++; lane4 [b[ 9]]++;
        lane5 [b[10]]++; lane5 [b[11]]++;
        lane6 [b[12]]++; lane6 [b[13]]++;
        lane7 [b[14]]++; lane7 [b[15]]++;
        lane8 [b[16]]++; lane8 [b[17]]++;
        lane9 [b[18]]++; lane9 [b[19]]++;
        lane10[b[20]]++; lane10[b[21]]++;
        lane11[b[22]]++; lane11[b[23]]++;
        lane12[b[24]]++; lane12[b[25]]++;
        lane13[b[26]]++; lane13[b[27]]++;
        lane14[b[28]]++; lane14[b[29]]++;
        lane15[b[30]]++; lane15[b[31]]++;
    }

    // Scalar tail into lane0
    for (; i < n; ++i)
        lane0[data[i]]++;

    // Reduce 16 lane histograms into final count[]
    for (size_t b = 0; b < RADIX_KI16; ++b)
    {
        size_t s = 0;
        s += lane0 [b]; s += lane1 [b];
        s += lane2 [b]; s += lane3 [b];
        s += lane4 [b]; s += lane5 [b];
        s += lane6 [b]; s += lane7 [b];
        s += lane8 [b]; s += lane9 [b];
        s += lane10[b]; s += lane11[b];
        s += lane12[b]; s += lane13[b];
        s += lane14[b]; s += lane15[b];
        count[b] = s;
    }
}

#endif


