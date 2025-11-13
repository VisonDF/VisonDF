#pragma once

inline void radix_sort_uint8(const uint8_t* keys, size_t* idx, size_t n)
{
    constexpr size_t K = 256;
    size_t count[K] = {};
    size_t tmp_idx[n];

    for (size_t i = 0; i < n; i++)
        count[keys[idx[i]]]++;

    size_t sum = 0;
    for (size_t i = 0; i < K; i++) {
        size_t tmp = count[i];
        count[i] = sum;
        sum += tmp;
    }

    for (size_t i = 0; i < n; i++) {
        uint8_t key = keys[idx[i]];
        tmp_idx[count[key]++] = idx[i];
    }

    memcpy(idx, tmp_idx, n * sizeof(size_t));
}


