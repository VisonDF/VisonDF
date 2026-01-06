#pragma once

template <typename T, 
          bool NeedsNormalization = false>
inline void get_filtered_col_64(
                                const std::vector<T>& col_vec,
                                std::vector<T>& rtn_v,
                                const std::vector<uint8_t>& mask,
                                const unsigned int strt_vl,
                                const size_t strt,
                                const size_t end,
                                const size_t out_idx_vl
                                )
{
    size_t out_idx = out_idx_vl;
    size_t i = strt;

    #if defined(__AVX512F__)
    for (; i + 32 <= end; i += 32)
    {
        __m256i mbytes = _mm256_loadu_si256((const __m256i*)&mask[i]);
    
        __mask32 maskbits;
        if constexpr (NeedsNormalization) {
            __m256i nz = _mm256_cmpgt_epi8(mbytes, _mm256_setzero_si256());
            maskbits = _mm256_movemask_epi8(nz);
        } else {
            maskbits = _mm256_movemask_epi8(mbytes);
        }
    
        __mmask8 k0 = (__mmask8)( maskbits        & 0xFFu );
        __mmask8 k1 = (__mmask8)((maskbits >> 8)  & 0xFFu );
        __mmask8 k2 = (__mmask8)((maskbits >> 16) & 0xFFu );
        __mmask8 k3 = (__mmask8)((maskbits >> 24) & 0xFFu );

        if constexpr (std::is_same_v<T, double>) {
            __m512d vals0 = _mm512_loadu_pd(&col_vec[strt_vl + i]);
            _mm512_mask_compressstoreu_pd(
                &rtn_v[out_idx],
                k0,
                vals0
            );
        } else {
            __m512i vals0 = _mm512_loadu_si512(&col_vec[strt_vl + i]);
            _mm512_mask_compressstoreu_epi64(
                (__m512i*)&rtn_v[out_idx],
                k0,
                vals0
            );
        }
        out_idx += _mm_popcnt_u32(k0);

        if constexpr (std::is_same_v<T, double>) {
            __m512d vals1 = _mm512_loadu_pd(&col_vec[strt_vl + i + 8]); 
            _mm512_mask_compressstoreu_pd(
                &rtn_v[out_idx],
                k1,
                vals1
            );
        } else {
            __m512i vals1 = _mm512_loadu_si512(&col_vec[strt_vl + i + 8]); 
            _mm512_mask_compressstoreu_epi64(
                (__m512i*)&rtn_v[out_idx],
                k1,
                vals1
            );
        }
        out_idx += _mm_popcnt_u32(k1);

        if constexpr (std::is_same_v<T, double>) {
            __m512d vals2 = _mm512_loadu_pd(&col_vec[strt_vl + i + 16]); 
            _mm512_mask_compressstoreu_pd(
                &rtn_v[out_idx],
                k2,
                vals2
            );
        } else {
            __m512i vals2 = _mm512_loadu_si512(&col_vec[strt_vl + i + 16]); 
            _mm512_mask_compressstoreu_epi64(
                (__m512i*)&rtn_v[out_idx],
                k2,
                vals2
            );
        }
        out_idx += _mm_popcnt_u32(k2);

        if constexpr (std::is_same_v<T, double>) {
            __m512d vals3 = _mm512_loadu_pd(&col_vec[strt_vl + i + 24]); 
            _mm512_mask_compressstoreu_pd(
                &rtn_v[out_idx],
                k3,
                vals3
            );
        } else {
            __m512i vals3 = _mm512_loadu_si512(&col_vec[strt_vl + i + 24]); 
            _mm512_mask_compressstoreu_epi64(
                (__m512i*)&rtn_v[out_idx],
                k3,
                vals3
            );
        }
        out_idx += _mm_popcnt_u32(k3);

    }
    #elif defined(__AVX2__)
    for (; i + 32 <= end; i += 32)
    {
        // Load 32 mask bytes → 32 bits
        __m256i mbytes = _mm256_loadu_si256(
            reinterpret_cast<const __m256i*>(&mask[i])
        );
    
        uint32_t maskbits;
        if constexpr (NeedsNormalization) {
            __m256i zero = _mm256_setzero_si256();
            __m256i nz   = _mm256_cmpgt_epi8(mbytes, zero);   
            maskbits = static_cast<uint32_t>(_mm256_movemask_epi8(nz));
        } else {
            maskbits = static_cast<uint32_t>(_mm256_movemask_epi8(mbytes));
        }
    
        // 8 groups of 4 elements, 4 bits each
        uint8_t m0 =  (maskbits >>  0) & 0x0Fu;                     
        uint8_t m1 =  (maskbits >>  4) & 0x0Fu;                     
        uint8_t m2 =  (maskbits >>  8) & 0x0Fu;                     
        uint8_t m3 =  (maskbits >> 12) & 0x0Fu;                     
        uint8_t m4 =  (maskbits >> 16) & 0x0Fu;                     
        uint8_t m5 =  (maskbits >> 20) & 0x0Fu;                     
        uint8_t m6 =  (maskbits >> 24) & 0x0Fu;                     
        uint8_t m7 =  (maskbits >> 28) & 0x0Fu;                     
    
        // Compress each group of 4 elements
        out_idx += compress4_lut(&col_vec[strt_vl + i +  0], m0, &rtn_v[out_idx]);
        out_idx += compress4_lut(&col_vec[strt_vl + i +  4], m1, &rtn_v[out_idx]);
        out_idx += compress4_lut(&col_vec[strt_vl + i +  8], m2, &rtn_v[out_idx]);
        out_idx += compress4_lut(&col_vec[strt_vl + i + 12], m3, &rtn_v[out_idx]);
        out_idx += compress4_lut(&col_vec[strt_vl + i + 16], m4, &rtn_v[out_idx]);
        out_idx += compress4_lut(&col_vec[strt_vl + i + 20], m5, &rtn_v[out_idx]);
        out_idx += compress4_lut(&col_vec[strt_vl + i + 24], m6, &rtn_v[out_idx]);
        out_idx += compress4_lut(&col_vec[strt_vl + i + 28], m7, &rtn_v[out_idx]);
    }
    #endif

    for (; i < end; ++i)
        if (mask[i])
            rtn_v[out_idx++] = col_vec[i];
}



