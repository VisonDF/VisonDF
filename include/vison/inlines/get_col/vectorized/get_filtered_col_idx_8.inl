#pragma once

template <typename T>
inline void get_filtered_col_idx_8(auto& col_vec,
                                    std::vector<T>& rtn_v,
                                    const std::vector<unsigned int>& mask)
{
    size_t i = 0;

    #if defined (__AVX512F__)

    for (; i + 16 <= n_el; i += 16)
    {
        __m512i idxv = _mm512_loadu_si512((const void*)&mask[i]);

        alignas(64) uint32_t idx_arr[16];
        _mm512_store_si512((__m512i*)idx_arr, idxv);

        alignas(16) unsigned char tmp[16];
        for (int k = 0; k < 16; ++k)
            tmp[k] = col_vec[idx_arr[k]];

        __m128i vals = _mm_loadu_si128((const __m128i*)tmp);
        _mm_storeu_si128((__m128i*)&rtn_v[i], vals);
    }

    #elif defined(__AVX2__)
    for (; i + 8 <= n_el; i += 8)
    {
        __m256i idxv = _mm256_loadu_si256((__m256i const*)&mask[i]);

        alignas(32) uint32_t idx_arr[8];
        _mm256_store_si256((__m256i*)idx_arr, idxv);

        alignas(16) unsigned char tmp[16];
        for (int k = 0; k < 8; ++k)
            tmp[k] = col_vec[idx_arr[k]];

        __m128i vals = _mm_loadl_epi64((__m128i*)tmp);
        _mm_storel_epi64((__m128i*)&rtn_v[i], vals);
    }
    #endif

    for (; i < n_el; ++i)
        rtn_v[i] = col_vec[mask[i]];
};


