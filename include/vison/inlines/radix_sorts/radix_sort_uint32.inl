#pragma once

inline void radix_sort_uint32(const uint32_t* keys, size_t* idx, size_t n)
{
    static constexpr size_t K = 256;
    size_t count[K];
    size_t tmp_idx[n];

    for (size_t pass = 0; pass < 4; pass++)
    {
        memset(count, 0, sizeof(count));

        size_t shift = pass * 8;

        for (size_t i = 0; i < n; i++)
            count[(keys[idx[i]] >> shift) & 0xFF]++;

        size_t sum = 0;
        for (size_t i = 0; i < K; i++) {
            size_t tmp = count[i];
            count[i] = sum;
            sum += tmp;
        }

        for (size_t i = 0; i < n; i++) {
            uint32_t key = keys[idx[i]];
            uint32_t bucket = (key >> shift) & 0xFF;
            tmp_idx[count[bucket]++] = idx[i];
        }

        memcpy(idx, tmp_idx, n * sizeof(size_t));
    }
}


