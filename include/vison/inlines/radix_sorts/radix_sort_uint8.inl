#pragma once

template <bool Simd = true>
inline void radix_sort_uint8(const uint8_t* keys,
                              size_t* idx,
                              size_t n)
{
    if (n == 0) return;

    std::vector<size_t> count(RADIX_KI8, 0);
    std::vector<size_t> tmp(n);

    // Histogram
    if constexpr (Simd) {
        #if defined(__AVX512F__)

            histogram_pass_u8_avx512_16buckets(tkeys.data(), n, shift, count.data());
            
        #elif defined(__AVX2__)

            if (n < 200'000) {
                histogram_pass_8_avx2(tkeys.data(), n, shift, count.data());
            } else {
                histogram_pass_u8_avx2_8buckets(tkeys.data(), n, shift, count.data());
            }

        #endif
    } else {
        for (size_t i = 0; i < n; i++) {
            uint8_t k = keys[i];
            count[k]++;
        }
    }

    // Prefix sum
    size_t sum = 0;
    for (size_t i = 0; i < radix_KI8; i++) {
        size_t c = count[i];
        count[i] = sum;
        sum += c;
    }

    // Scatter
    for (size_t i = 0; i < n; i++) {
        uint8_t k = keys[i];
        tmp[count[k]++] = i;
    }

    memcpy(idx, tmp.data(), n * sizeof(size_t));
}



