#pragma once

template <bool Simd = true>
inline void radix_sort_uint8(const uint8_t* keys,
                              size_t* idx,
                              size_t n)
{
    if (n == 0) return;

    std::vector<size_t> count(RADIX_KI8, 0);

    // Histogram
    if constexpr (Simd) {
        #if defined(__AVX512F__)

            histogram_pass_u8_avx512_16buckets(keys, n, count.data());
            
        #elif defined(__AVX2__)

            if (n < 200'000) {
                histogram_pass_u8_avx2(keys, n, count.data());
            } else {
                histogram_pass_u8_avx2_8buckets(keys, n, count.data());
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
    for (size_t i = 0; i < RADIX_KI8; i++) {
        size_t c = count[i];
        count[i] = sum;
        sum += c;
    }

    // Scatter

    #if defined(__AVX512F__)
    if constexpr (Simd) {
        scatter_pass_u8_avx512(keys, 
                                idx, 
                                n, 
                                count.data());
    } else
    #endif
    {
        for (size_t i = 0; i < n; i++) {
            uint8_t k = keys[i];
            idx[count[k]++] = i;
        }
    }

}



