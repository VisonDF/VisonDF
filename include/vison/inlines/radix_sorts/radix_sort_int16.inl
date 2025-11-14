#pragma once

template <bool Simd = true>
inline void radix_sort_int16(const int16_t* keys,
                              size_t* idx,
                              size_t n)
{
    if (n == 0) return;

    std::vector<size_t> count(RADIX_KI16, 0);
    std::vector<uint16_t> tkeys(n);

    for (size_t i = 0; i < n; i++)
        tkeys[i] = uint16_t(keys[i]) ^ 0x8000u;

    // Histogram
    if constexpr (Simd) {
        #if defined(__AVX512F__)

            histogram_pass_u16_avx512_16buckets(tkeys.data(), n, count.data());
            
        #elif defined(__AVX2__)

            if (n < 200'000) {
                histogram_pass_u16_avx2(tkeys.data(), n, count.data());
            } else {
                histogram_pass_u16_avx2_8buckets(tkeys.data(), n, count.data());
            }

        #endif
    } else {
        for (size_t i = 0; i < n; i++) {
            uint16_t k = tkeys[i];
            count[k]++;
        }
    }

    // Prefix sum
    size_t sum = 0;
    for (size_t i = 0; i < RADIX_KI16; i++) {
        size_t c = count[i];
        count[i] = sum;
        sum += c;
    }

    // Scatter

    #if defined(__AVX512F__)
    if constexpr (Simd) {
        scatter_pass_u16_avx512(tkeys.data(), 
                                idx, 
                                n, 
                                count.data());
    } else
    #endif
    {
        for (size_t i = 0; i < n; i++) {
            uint16_t k = tkeys[i];
            idx[count[k]++] = i;
        }
    }

}



