#pragma once

template <bool Simd = true, bool IsBoolCompressed = false>
inline void radix_sort_bool_u8(uint8_t* keys, 
                            size_t* idx, 
                            size_t n) {

    if constexpr (Simd) {
    #if defined(__AVX512F__)

        if constexpr (IsBoolCompressed) {

                avx512_bool_compressed(keys, n, idx);

        } else {

                avx512_bool_u8(keys, n, idx);

        }

    #elif defined(__AVX2___)

        if constexpr (IsBoolCompressed) {

                avx2_bool_compressed(keys, n, idx);

        } else {

                avx2_bool_u8(keys, n, idx);

        }

    #endif

    } else {

            size_t lo = 0;
            size_t hi = n;
            
            for (size_t i = 0; i < n; ++i) {
                if (!keys[i]) {
                    idx[lo++] = i;      
                } else {
                    idx[--hi] = i;      
                }
            }   

        }
    
}


