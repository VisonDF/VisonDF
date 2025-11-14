#pragma once

inline void histogram_pass_u16_avx2(
    const uint16_t* data,
    size_t n,
    size_t* count
)
{
    //alignas(64) size_t local[RADIX_KI16];
    //memset(local, 0, sizeof(local));

    size_t i = 0;

    for (; i + 16 <= n; i += 16)
    {
        __builtin_prefetch(data + i + 32, 0, 1);

        // Load 16 × uint16_t = 256 bits
        __m256i v = _mm256_loadu_si256((const __m256i*)(data + i));

        // Store into CPU-visible array
        alignas(32) uint16_t b[16];
        _mm256_store_si256((__m256i*)b, v);

        // Increment buckets
        count[b[0]]++;  count[b[1]]++;  count[b[2]]++;  count[b[3]]++;
        count[b[4]]++;  count[b[5]]++;  count[b[6]]++;  count[b[7]]++;
        count[b[8]]++;  count[b[9]]++;  count[b[10]]++; count[b[11]]++;
        count[b[12]]++; count[b[13]]++; count[b[14]]++; count[b[15]]++;
    }

    for (; i < n; i++)
        count[data[i]]++;

    //for (size_t k = 0; k < RADIX_KI16; k++)
    //    count[k] = local[k];
    
}



