#pragma once

inline void histogram_pass_u8_avx2(
    const uint8_t* data,
    size_t n,
    size_t* count
)
{
    alignas(64) size_t local[RADIX_KI8];
    memset(local, 0, sizeof(local));

    size_t i = 0;

    for (; i + 32 <= n; i += 32)
    {

        __builtin_prefetch(data + i + 64, 0, 1);
        
        __m256i v = _mm256_loadu_si256((const __m256i*)(data + i));

        alignas(32) uint8_t b[32];
        _mm256_store_si256((__m256i*)b, v);

        local[b[0]]++;  local[b[1]]++;  local[b[2]]++;  local[b[3]]++;
        local[b[4]]++;  local[b[5]]++;  local[b[6]]++;  local[b[7]]++;
        local[b[8]]++;  local[b[9]]++;  local[b[10]]++; local[b[11]]++;
        local[b[12]]++; local[b[13]]++; local[b[14]]++; local[b[15]]++;
        local[b[16]]++; local[b[17]]++; local[b[18]]++; local[b[19]]++;
        local[b[20]]++; local[b[21]]++; local[b[22]]++; local[b[23]]++;
        local[b[24]]++; local[b[25]]++; local[b[26]]++; local[b[27]]++;
        local[b[28]]++; local[b[29]]++; local[b[30]]++; local[b[31]]++;
    }

    for (; i < n; i++)
        local[data[i]]++;

    for (size_t b = 0; b < RADIX_KI8; b++)
        count[b] = local[b];
}



