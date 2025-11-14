#pragma once

inline void histogram_pass_u8_avx2_8buckets(
    const uint8_t* data,
    size_t n,
    size_t* count  
)
{

    size_t* __restrict local = get_local_histogram_8x_u8();
    memset(local, 0, RADIX_LANES * RADIX_KI32 * sizeof(size_t));

    // lane pointers
    size_t* lane0 = local + 0 * RADIX_KI8;
    size_t* lane1 = local + 1 * RADIX_KI8;
    size_t* lane2 = local + 2 * RADIX_KI8;
    size_t* lane3 = local + 3 * RADIX_KI8;
    size_t* lane4 = local + 4 * RADIX_KI8;
    size_t* lane5 = local + 5 * RADIX_KI8;
    size_t* lane6 = local + 6 * RADIX_KI8;
    size_t* lane7 = local + 7 * RADIX_KI8;

    size_t i = 0;

    for (; i + 32 <= n; i += 32)
    {

        __builtin_prefetch(data + i + 64, 0, 1);

        __m256i v = _mm256_loadu_si256((__m256i const*)(data + i));

        // store to stack
        alignas(32) uint8_t b[32];
        _mm256_store_si256((__m256i*)b, v);

        // increment 32 items → 4 per lane × 8 lanes
        lane0[b[0]]++;  lane0[b[1]]++;  lane0[b[2]]++;  lane0[b[3]]++;

        lane1[b[4]]++;  lane1[b[5]]++;  lane1[b[6]]++;  lane1[b[7]]++;

        lane2[b[8]]++;  lane2[b[9]]++;  lane2[b[10]]++; lane2[b[11]]++;

        lane3[b[12]]++; lane3[b[13]]++; lane3[b[14]]++; lane3[b[15]]++;

        lane4[b[16]]++; lane4[b[17]]++; lane4[b[18]]++; lane4[b[19]]++;

        lane5[b[20]]++; lane5[b[21]]++; lane5[b[22]]++; lane5[b[23]]++;

        lane6[b[24]]++; lane6[b[25]]++; lane6[b[26]]++; lane6[b[27]]++;

        lane7[b[28]]++; lane7[b[29]]++; lane7[b[30]]++; lane7[b[31]]++;
    }

    for (; i < n; i++)
        lane0[data[i]]++;

    for (size_t b = 0; b < RADIX_KI8; b++)
    {
        count[b] =
            lane0[b] + lane1[b] + lane2[b] + lane3[b] +
            lane4[b] + lane5[b] + lane6[b] + lane7[b];
    }
}





