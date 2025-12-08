#pragma once

inline void histogram_pass_u8_avx2(
    const uint8_t* data,
    size_t n,
    size_t* count
)
{
    size_t i = 0;

    for (; i + 32 <= n; i += 32)
    {

        __builtin_prefetch(data + i + 64, 0, 1);
        
        __m256i v = _mm256_loadu_si256((const __m256i*)(data + i));

        alignas(32) uint8_t b[32];
        _mm256_store_si256((__m256i*)b, v);

        count[b[0]]++;  count[b[1]]++;  count[b[2]]++;  count[b[3]]++;
        count[b[4]]++;  count[b[5]]++;  count[b[6]]++;  count[b[7]]++;
        count[b[8]]++;  count[b[9]]++;  count[b[10]]++; count[b[11]]++;
        count[b[12]]++; count[b[13]]++; count[b[14]]++; count[b[15]]++;
        count[b[16]]++; count[b[17]]++; count[b[18]]++; count[b[19]]++;
        count[b[20]]++; count[b[21]]++; count[b[22]]++; count[b[23]]++;
        count[b[24]]++; count[b[25]]++; count[b[26]]++; count[b[27]]++;
        count[b[28]]++; count[b[29]]++; count[b[30]]++; count[b[31]]++;
    }

    for (; i < n; i++)
        count[data[i]]++;

}



