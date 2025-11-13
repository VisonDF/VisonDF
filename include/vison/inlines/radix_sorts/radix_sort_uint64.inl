#pragma once

inline void radix_sort_uint64(const uint64_t* keys, size_t* idx, size_t n)
{
    constexpr size_t K = 1 << 16;      
    size_t* count = new size_t[K];
    size_t* tmp_idx = new size_t[n];

    for (size_t pass = 0; pass < 4; pass++)
    {
        memset(count, 0, K * sizeof(size_t));
        size_t shift = pass * 16;

        
        for (size_t i = 0; i < n; i++)
            count[(keys[idx[i]] >> shift) & 0xFFFF]++;

        size_t sum = 0;
        for (size_t i = 0; i < K; i++) {
            size_t tmp = count[i];
            count[i] = sum;
            sum += tmp;
        }

        for (size_t i = 0; i < n; i++) {
            uint64_t key = keys[idx[i]];
            uint32_t bucket = (key >> shift) & 0xFFFF;
            tmp_idx[count[bucket]++] = idx[i];
        }

        memcpy(idx, tmp_idx, n * sizeof(size_t));
    }

    delete[] count;
    delete[] tmp_idx;
}


