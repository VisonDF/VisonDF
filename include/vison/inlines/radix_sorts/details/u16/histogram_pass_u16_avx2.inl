#pragma once

inline void histogram_pass_u16_avx2(
    const uint16_t* data,
    size_t n,
    size_t* count
)
{
    alignas(64) size_t local[RADIX_KI16];
    memset(local, 0, sizeof(local));

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
        local[b[0]]++;  local[b[1]]++;  local[b[2]]++;  local[b[3]]++;
        local[b[4]]++;  local[b[5]]++;  local[b[6]]++;  local[b[7]]++;
        local[b[8]]++;  local[b[9]]++;  local[b[10]]++; local[b[11]]++;
        local[b[12]]++; local[b[13]]++; local[b[14]]++; local[b[15]]++;
    }

    for (; i < n; i++)
        local[data[i]]++;

    for (size_t k = 0; k < RADIX_KI16; k++)
        count[k] = local[k];
    
}



