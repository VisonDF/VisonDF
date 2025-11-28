#pragma once

template <typename T>
inline void get_filtered_col_idx_16(const T* __restrict col_vec,
                                    std::vector<T>& rtn_v,
                                    const std::vector<unsigned int>& mask)
{

    size_t i = 0;

    #if defined (__AVX512F__)
    for (; i + 16 <= n_el; i += 16)
    {
        // Load 16×32-bit indices
        __m512i idxv = _mm512_loadu_si512((const __m512i*)&mask[i]);

        // Extract raw indices
        alignas(64) uint32_t idx_arr[16];
        _mm512_store_si512((__m512i*)idx_arr, idxv);

        // Gather into temps (16 values of 16-bit)
        alignas(32) uint16_t tmp[16];
        for (int k = 0; k < 16; ++k)
            tmp[k] = col_vec[idx_arr[k]];

        // Load 16×16-bit into an XMM register
        __m256i vals = _mm_load_si256((__m256i*)tmp);

        // Store them to output
        _mm_storeu_si256((__m256i*)&rtn_v[i], vals);
    }

    #elif defined(__AVX2__)
    for (; i + 8 <= n_el; i += 8)
    {
        // Load 8×32-bit indices
        __m256i idxv = _mm256_loadu_si256((const __m256i*)&mask[i]);

        // Extract raw indices
        alignas(32) uint32_t idx_arr[8];
        _mm256_store_si256((__m256i*)idx_arr, idxv);

        // Gather into temps (8 values of 16-bit)
        alignas(16) uint16_t tmp[8];
        for (int k = 0; k < 8; ++k)
            tmp[k] = col_vec[idx_arr[k]];

        // Load 8×16-bit into an XMM register
        __m128i vals = _mm_load_si128((__m128i*)tmp);

        // Store them to output
        _mm_storeu_si128((__m128i*)&rtn_v[i], vals);
    }
    #endif

    // scalar remainder
    for (; i < n_el; ++i)
        rtn_v[i] = col_vec[mask[i]];
}


