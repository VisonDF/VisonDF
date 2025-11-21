#pragma once

template <bool Simd = true>
inline void radix_sort_uint16(const uint16_t* keys,
                              size_t* idx,
                              size_t n)
{

    #if !defined(__AVX2__)
    static_assert(!Simd, 
        "Simd=true requires AVX2, but AVX2 is not available on this CPU/compiler.");
    #endif

    if (n == 0) {
        warn("0 rows in radix_sort_uint16");
        return;
    }

    std::vector<size_t> count(RADIX_KI16, 0);

    // Histogram
    if constexpr (Simd) {
        #if defined(__AVX512F__)

            histogram_pass_u16_avx512_16buckets(keys, n, count.data());
            
        #elif defined(__AVX2__)

            if (n < 200'000) {
                histogram_pass_u16_avx2(keys, n, count.data());
            } else {
                histogram_pass_u16_avx2_8buckets(keys, n, count.data());
            }

        #endif
    } else {
        for (size_t i = 0; i < n; i++) {
            uint16_t k = keys[i];
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
        scatter_pass_u16_avx512(keys, 
                                idx, 
                                n, 
                                count.data());
    } else
    #endif
    {
        for (size_t i = 0; i < n; i++) {
            uint16_t k = keys[i];
            idx[count[k]++] = i;
        }
    }

}



