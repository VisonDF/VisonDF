#pragma once

inline void radix_sort_int64(const int64_t* keys,
                             size_t* idx,
                             size_t n)
{
    using U = uint64_t;
    constexpr size_t K = 1 << 16;     
    constexpr size_t PASSES = 4;      

    std::vector<U> tkeys(n);
    std::vector<size_t> tmp(n);
    std::vector<size_t> count(K);

    // signed → unsigned transform
    for (size_t i = 0; i < n; i++)
        tkeys[i] = U(keys[idx[i]]) ^ 0x8000000000000000ull;

    for (size_t pass = 0; pass < PASSES; pass++)
    {
        size_t shift = pass * 16;

        // histogram
        memset(count.data(), 0, K * sizeof(size_t));
        for (size_t i = 0; i < n; i++)
            count[(tkeys[i] >> shift) & 0xFFFF]++;

        // prefix sum
        size_t sum = 0;
        for (size_t i = 0; i < K; i++) {
            size_t c = count[i];
            count[i] = sum;
            sum += c;
        }

        // reorder
        for (size_t i = 0; i < n; i++) {
            U key = tkeys[i];
            size_t b = (key >> shift) & 0xFFFF;
            tmp[count[b]++] = idx[i];
        }

        memcpy(idx, tmp.data(), n * sizeof(size_t));

        // update transformed keys ordering
        for (size_t i = 0; i < n; i++)
            tkeys[i] = (uint64_t(keys[idx[i]]) ^ 0x8000000000000000ull);
    }
}



