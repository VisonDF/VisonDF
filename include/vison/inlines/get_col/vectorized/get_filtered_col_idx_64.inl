#pragma once

template <typename T>
inline void get_filtered_col_idx_64(
    const std::vector<T>& src,
    std::vector<T>& rtn_v,
    const std::vector<unsigned int>& mask)
{

    const size_t n_el = mask.size();
    size_t i = 0;

    #if defined(__AVX512F__)
    for (; i + 16 <= n_el; i += 16) {

        __m512i idx = _mm512_loadu_si512((const void*)&mask[i]);

        if constexpr (std::is_same_v<T, double>) {
            __m512d vals = _mm512_i32gather_pd(idx, src.data(), 8);
            _mm512_storeu_pd(&rtn_v[i], vals);

        } else {
            __m512i vals = _mm512_i32gather_epi64(idx, src.data(), 8);
            _mm512_storeu_si512((__m512i*)&rtn_v[i], vals);
        }
    }

    #elif defined(__AVX2__)
    for (; i + 4 <= n_el; i += 4) {

        __m128i idx = _mm_loadu_si128((const __m128i*)&mask[i]);

        if constexpr (std::is_same_v<T, double>) {
            __m256d vals = _mm256_i32gather_pd(src.data(), idx, 8);
            _mm256_storeu_pd(&rtn_v[i], vals);

        } else {
            __m256i vals = _mm256_i32gather_epi64(src.data(), idx, 8);
            _mm256_storeu_si256((__m256i*)&rtn_v[i], vals);
        }
    }
    #endif

    for (; i < n_el; i++)
        rtn_v[i] = src[mask[i]];
}


