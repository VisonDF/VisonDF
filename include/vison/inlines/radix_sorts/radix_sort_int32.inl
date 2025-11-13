#pragma once

inline void radix_sort_int32(const int32_t* keys,
                             size_t* idx,
                             size_t n)
{

    using U = uint32_t;
    constexpr size_t K = 1 << 16;     
    constexpr size_t PASSES = 2;      

    std::vector<U> tkeys(n);
    std::vector<size_t> tmp(n);
    std::vector<size_t> count(K);

    // signed → unsigned transform
    for (size_t i = 0; i < n; i++)
        tkeys[i] = U(keys[idx[i]]) ^ 0x80000000u;

    for (size_t pass = 0; pass < PASSES; pass++)
    {
        size_t shift = pass * 16;

        memset(count.data(), 0, K * sizeof(size_t));
        for (size_t i = 0; i < n; i++)
            count[(tkeys[i] >> shift) & 0xFFFF]++;

        size_t sum = 0;
        for (size_t i = 0; i < K; i++) {
            size_t c = count[i];
            count[i] = sum;
            sum += c;
        }

        for (size_t i = 0; i < n; i++) {
            U key = tkeys[i];
            size_t b = (key >> shift) & 0xFFFF;
            tmp[count[b]++] = idx[i];
        }

        memcpy(idx, tmp.data(), n * sizeof(size_t));

        for (size_t i = 0; i < n; i++)
            tkeys[i] = (uint32_t(keys[idx[i]]) ^ 0x80000000u);
    }
}


