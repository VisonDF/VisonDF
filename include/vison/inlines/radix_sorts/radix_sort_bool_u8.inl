#pragma once

template <bool Simd = true, bool IsBoolCompressed = false>
inline void radix_sort_bool_u8(uint8_t* keys, 
                            size_t* idx, 
                            size_t n) {

    #if !defined(__AVX2__)
    static_assert(!Simd, 
        "Simd=true requires AVX2, but AVX2 is not available on this CPU/compiler.");
    #endif

    if (n == 0) {
        warn("0 rows in radix_sort_bool_u8");
        return;
    }

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


            if constexpr (IsBoolCompressed) {
              std::cerr << "Error, IsBoolCompressed must be aplied with SIMD";
              return;
            }

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


