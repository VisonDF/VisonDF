#pragma once

inline void histogram_pass_u16_avx2_8buckets(
    const uint16_t* __restrict data,
    size_t n,
    size_t* __restrict count
)
{
    size_t* __restrict local = get_local_histogram_8x_u16();
    memset(local, 0, RADIX_LANES * RADIX_KI16 * sizeof(size_t));

    // lane pointers
    size_t* lane0 = local + 0 * RADIX_KI16;
    size_t* lane1 = local + 1 * RADIX_KI16;
    size_t* lane2 = local + 2 * RADIX_KI16;
    size_t* lane3 = local + 3 * RADIX_KI16;
    size_t* lane4 = local + 4 * RADIX_KI16;
    size_t* lane5 = local + 5 * RADIX_KI16;
    size_t* lane6 = local + 6 * RADIX_KI16;
    size_t* lane7 = local + 7 * RADIX_KI16;

    size_t i = 0;

    // here we simulate 2 iterations inside one iteration to 
    // make better unrolling, increase posible ILP instructions
    for (; i + 32 <= n; i += 32)
    {
        __builtin_prefetch(data + i + 64, 0, 1);

        __m256i v1 = _mm256_loadu_si256((const __m256i*)(data + i));
        __m256i v2 = _mm256_loadu_si256((const __m256i*)(data + i + 16));

        alignas(32) uint16_t b1[16];
        alignas(32) uint16_t b2[16];

        _mm256_store_si256((__m256i*)b1, v1);
        _mm256_store_si256((__m256i*)b2, v2);

        lane0[b1[0]]++; lane0[b1[1]]++;
        lane0[b2[0]]++; lane0[b2[1]]++;

        lane1[b1[2]]++; lane1[b1[3]]++;
        lane1[b2[2]]++; lane1[b2[3]]++;

        lane2[b1[4]]++; lane2[b1[5]]++;
        lane2[b2[4]]++; lane2[b2[5]]++;

        lane3[b1[6]]++; lane3[b1[7]]++;
        lane3[b2[6]]++; lane3[b2[7]]++;

        lane4[b1[8]]++; lane4[b1[9]]++;
        lane4[b2[8]]++; lane4[b2[9]]++;

        lane5[b1[10]]++; lane5[b1[11]]++;
        lane5[b2[10]]++; lane5[b2[11]]++;

        lane6[b1[12]]++; lane6[b1[13]]++;
        lane6[b2[12]]++; lane6[b2[13]]++;

        lane7[b1[14]]++; lane7[b1[15]]++;
        lane7[b2[14]]++; lane7[b2[15]]++;
    }

    for (; i < n; i++)
        lane0[data[i]]++;

    for (size_t b = 0; b < RADIX_KI16; b++)
    {
        count[b] =
            lane0[b] + lane1[b] + lane2[b] + lane3[b] +
            lane4[b] + lane5[b] + lane6[b] + lane7[b];
    }
}





