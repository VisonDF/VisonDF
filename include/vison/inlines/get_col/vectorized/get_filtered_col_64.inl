#pragma once

// Compress 4x64-bit chunk according to 4-bit mask, scalar but branch-light.
template <typename T>
inline int compress4x64_lut(const T* src, 
                            uint8_t mask4, 
                            T* dst) {
    const Mask4LUT &e = LUT4[mask4];
    int n = e.count;
    if (n >= 1) dst[0] = src[e.idx[0]];
    if (n >= 2) dst[1] = src[e.idx[1]];
    if (n >= 3) dst[2] = src[e.idx[2]];
    if (n >= 4) dst[3] = src[e.idx[3]];
    return n;
}

template <typename T, bool NeedsNormalization = false>
inline void get_filtered_col_64(
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

        __m512i vals0 = _mm512_loadu_si512((const __m512i*)&col_vec[i]);
        if constexpr (std::is_same_v<T, double>) {
            _mm512_mask_compressstoreu_pd(
                &rtn_v[out_idx],
                k0,
                __m512_castsi512_pd(vals0)
            );
        } else {
            _mm512_mask_compressstoreu_epi64(
                (__m512i*)&rtn_v[out_idx],
                k0,
                vals0
            );
        }
        out_idx += _mm_popcnt_u32(k0);

        __m512i vals1 = _mm512_loadu_si512((const __m512i*)&col_vec[i + 8]); 
        if constexpr (std::is_same_v<T, double>) {
            _mm512_mask_compressstoreu_pd(
                &rtn_v[out_idx],
                k1,
                __m512_castsi512_pd(vals1)
            );
        } else {
            _mm512_mask_compressstoreu_epi64(
                (__m512i*)&rtn_v[out_idx],
                k1,
                vals1
            );
        }
        out_idx += _mm_popcnt_u32(k1);

        __m512i vals2 = _mm512_loadu_si512((const __m512i*)&col_vec[i + 16]); 
        if constexpr (std::is_same_v<T, double>) {
            _mm512_mask_compressstoreu_pd(
                &rtn_v[out_idx],
                k2,
                __m512_castsi512_pd(vals2)
            );
        } else {
            _mm512_mask_compressstoreu_epi64(
                (__m512i*)&rtn_v[out_idx],
                k2,
                vals2
            );
        }
        out_idx += _mm_popcnt_u32(k2);

        __m512i vals3 = _mm512_loadu_si512((const __m512i*)&col_vec[i + 24]); 
        if constexpr (std::is_same_v<T, double>) {
            _mm512_mask_compressstoreu_pd(
                &rtn_v[out_idx],
                k3,
                __m512_castsi512_pd(vals3)
            );
        } else {
            _mm512_mask_compressstoreu_epi64(
                (__m512i*)&rtn_v[out_idx],
                k3,
                vals3
            );
        }
        out_idx += _mm_popcnt_u32(k3);

    }
    #elif defined(__AVX2__)
    for (; i + 32 <= n_el; i += 32)
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
        out_idx += compress4x64_lut(&col_vec[i +  0], m0, &rtn_v[out_idx]);
        out_idx += compress4x64_lut(&col_vec[i +  4], m1, &rtn_v[out_idx]);
        out_idx += compress4x64_lut(&col_vec[i +  8], m2, &rtn_v[out_idx]);
        out_idx += compress4x64_lut(&col_vec[i + 12], m3, &rtn_v[out_idx]);
        out_idx += compress4x64_lut(&col_vec[i + 16], m4, &rtn_v[out_idx]);
        out_idx += compress4x64_lut(&col_vec[i + 20], m5, &rtn_v[out_idx]);
        out_idx += compress4x64_lut(&col_vec[i + 24], m6, &rtn_v[out_idx]);
        out_idx += compress4x64_lut(&col_vec[i + 28], m7, &rtn_v[out_idx]);
    }
    #endif

    for (; i < n_el; ++i)
        if (mask[i])
            rtn_v[out_idx++] = col_vec[i];
}



