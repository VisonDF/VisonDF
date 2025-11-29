#pragma once

template <typename T, bool NeedsNormalization = false>
inline void get_filtered_col_16(
    const std::vector<T>& col_vec,
    std::vector<T>& rtn_v,
    const std::vector<uint8_t>& mask,
    const size_t n_el)
{
    size_t out_idx = 0;
    size_t i = 0;

    #if defined(__AVX512F__)
    for (; i + 32 <= n_el; i += 32)
    {
        __m256i mbytes = _mm256_loadu_si256((const __m256i*)&mask[i]);
    
        __mmask32 k;
        if constexpr (NeedsNormalization) {
            __m256i nz = _mm256_cmpgt_epi8(mbytes, _mm256_setzero_si256());
            k = _mm256_movemask_epi8(nz);
        } else {
            k = _mm256_movemask_epi8(mbytes);
        }
    
        __m512i vals = _mm512_loadu_si512((const __m512i*)&col_vec[i]);
    
        _mm512_mask_compressstoreu_epi16(
            &rtn_v[out_idx],
            k,
            vals
        );
    
        out_idx += _mm_popcnt_u32(k);
    }
    #elif defined(__AVX2__)
    for (; i + 32 <= n_el; i += 32)
    {
        __m256i mbytes = _mm256_loadu_si256((const __m256i*)&mask[i]);
    
        uint32_t maskbits;
        if constexpr (NeedsNormalization) {
            __m256i nz = _mm256_cmpgt_epi8(mbytes, _mm256_setzero_si256());
            maskbits = _mm256_movemask_epi8(nz);
        } else {
            maskbits = _mm256_movemask_epi8(mbytes);
        }
    
        uint16_t mask_lo = static_cast<uint16_t>( maskbits        & 0xFFFFu);
        uint16_t mask_hi = static_cast<uint16_t>((maskbits >> 16) & 0xFFFFu);
    
        __m256i vals0 = _mm256_loadu_si256((const __m256i*)&col_vec[i]);      // elements i..i+15
        __m256i vals1 = _mm256_loadu_si256((const __m256i*)&col_vec[i + 16]); // elements i+16..i+31
    
        alignas(32) uint16_t tmp[32];
        _mm256_storeu_si256((__m256i*)(tmp),      vals0);
        _mm256_storeu_si256((__m256i*)(tmp + 16), vals1);
    
        for (int lane = 0; lane < 16; ++lane)
            if (mask_lo & (1u << lane))
                rtn_v[out_idx++] = tmp[lane];
    
        for (int lane = 0; lane < 16; ++lane)
            if (mask_hi & (1u << lane))
                rtn_v[out_idx++] = tmp[16 + lane];
    }
    #endif

    for (; i < n_el; ++i)
        if (mask[i])
            rtn_v[out_idx++] = col_vec[i];
}



