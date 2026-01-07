#pragma once

template <typename T>
inline void get_filtered_col_idx_32(const std::vector<T>& src,
                                    std::vector<T>& rtn_v,
                                    const std::vector<unsigned int>& mask,
                                    const size_t start,
                                    const size_t end)
{

    size_t i = start;

    #if defined (__AVX512F__)
    for (; i + 16 <= end; i += 16) {
    
        __m512i idx = _mm512_loadu_si512((const void*)&mask[i]);
    
        if constexpr (std::is_same_v<T, float>) {
            __m512 vals = _mm512_i32gather_ps(idx, src.data(), 4);
            _mm512_storeu_ps(&rtn_v[i], vals);
    
        } else {
            __m512i vals = _mm512_i32gather_epi32(idx, src.data(), 4);
            _mm512_storeu_si512((__m512i*)&rtn_v[i], vals);
        }
    }
    #elif defined(__AVX2__)
    for (; i + 8 <= end; i += 8) {
    
        __m256i idx = _mm256_loadu_si256((const void*)&mask[i]);
    
        if constexpr (std::is_same_v<T, float>) {
            __m256 vals = _mm256_i32gather_ps(src.data(), idx, 4);
            _mm256_storeu_ps(&rtn_v[i], vals);
    
        } else {
            __m256i vals = _mm256_i32gather_epi32(src.data(), idx, 4);
            _mm256_storeu_si256((__m256i*)&rtn_v[i], vals);
        }
    }
    #endif

    for (; i < end; i += 1)
        rtn_v[i] = src[mask[i]];

}



