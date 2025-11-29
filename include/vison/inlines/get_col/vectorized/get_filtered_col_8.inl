#pragma once

template <typename T, bool NeedsNormalization = true>
inline void get_filtered_col_8(
    const std::vector<T>& col_vec,
    std::vector<T>& rtn_v,
    const std::vector<uint8_t>& mask,
    const size_t n_el)
{
    size_t out_idx = 0;
    size_t i = 0;

    #if defined(__AVX512F__)
    for (; i + 64 <= n_el; i += 64)
    {
         // Load 64 mask bytes (values = 0 or 1)
        __m512i mbytes = _mm512_loadu_si512((const void*)&mask[i]);

        __mmask64 k;
        if constexpr (NeedsNormalization) {
        // Normalize: convert {0,1} and create a bitmask
            k = _mm512_cmpneq_epi8_mask(mbytes, _mm512_setzero_si512());
        } else if constexpr (!NeedsNormalization) {
            k = _mm512_movepi8_mask(mbytes);
        }

        // Load 64 source bytes
        __m512i vals = _mm512_loadu_si512((const void*)&col_vec[i]);

        // Compress-store only active bytes
        _mm512_mask_compressstoreu_epi8(
            &rtn_v[out_idx],
            k,
            vals
        );

        out_idx += _mm_popcnt_u64(k);

    }
    #elif defined(__AVX2__)
    for (; i + 32 <= n_el; i += 32)
    {
        // Load 32 mask bytes and 32 value bytes
        __m256i mbytes = _mm256_loadu_si256((const __m256i*)&mask[i]);
        __m256i vals   = _mm256_loadu_si256((const __m256i*)&col_vec[i]);
    
        uint32_t maskbits;
        // Produce 32-bit mask (with normalization, so it does not check wether 
        // negative or positive but if 0 or 1)
        if constexpr (NeedsNormalization) {
            maskbits =
                _mm256_movemask_epi8(
                    _mm256_cmpgt_epi8(mbytes, _mm256_setzero_si256())
                );
        } else if constexpr (!NeedsNormalization) {
            maskbits =
                _mm256_movemask_epi8(mbytes);
        }
    
        // Extract active bytes (~32 iterations)
        uint8_t tmp[32];
        _mm256_storeu_si256((__m256i*)tmp, vals);
    
        for (int k = 0; k < 32; ++k)
            if (maskbits & (1u << k))
                rtn_v[out_idx++] = tmp[k];
    }
    #endif

    for (; i < n_el; ++i)
        if (mask[i])
            rtn_v[out_idx++] = col_vec[i];
}



