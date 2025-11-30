#pragma once

template <typename T, bool NeedsNormalization = false>
inline void get_filtered_col_32(
    const std::vector<T>& col_vec,
    std::vector<T>& rtn_v,
    const std::vector<uint8_t>& mask,
    const unsigned int strt_vl, 
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
    
        __mmask16 k0 = (__mmask16)( maskbits        & 0xFFFFu );
        __mmask16 k1 = (__mmask16)((maskbits >> 16) & 0xFFFFu );
        
        if constexpr (std::is_same_v<T, float>) {
            __m512 vals0 = _mm512_loadu_ps(&col_vec[strt_vl + i]);
            _mm512_mask_compressstoreu_ps(
                &rtn_v[out_idx],
                k0,
                vals0
            );
        } else {
            __m512i vals0 = _mm512_loadu_si512(&col_vec[strt_vl + i]);
            _mm512_mask_compressstoreu_epi32(
                (__m512i*)&rtn_v[out_idx],
                k0,
                vals0
            );
        }
        out_idx += _mm_popcnt_u32(k0); //correct because implicit cast
    
        if constexpr (std::is_same_v<T, float>) {
            __m512 vals1 = _mm512_loadu_ps(&col_vec[strt_vl + i + 16]);
            _mm512_mask_compressstoreu_ps(
                &rtn_v[out_idx],
                k1,
                vals1
            );
        } else {
            __m512i vals1 = _mm512_loadu_si512(&col_vec[strt_vl + i + 16]);
            _mm512_mask_compressstoreu_epi32(
                (__m512i*)&rtn_v[out_idx],
                k1,
                vals1
            );
        }
        out_idx += _mm_popcnt_u32(k1); //correct because implicit cast
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
    
        uint8_t m0 = static_cast<uint8_t>( maskbits        & 0xFFu);
        uint8_t m1 = static_cast<uint8_t>((maskbits >> 8)  & 0xFFu);
        uint8_t m2 = static_cast<uint8_t>((maskbits >> 16) & 0xFFu);
        uint8_t m3 = static_cast<uint8_t>((maskbits >> 24) & 0xFFu);

        out_idx += compress8_lut(&col_vec[strt_vl + i +  0], m0, &rtn_v[out_idx]);
        out_idx += compress8_lut(&col_vec[strt_vl + i +  8], m1, &rtn_v[out_idx]);
        out_idx += compress8_lut(&col_vec[strt_vl + i + 16], m2, &rtn_v[out_idx]);
        out_idx += compress8_lut(&col_vec[strt_vl + i + 24], m3, &rtn_v[out_idx]);

    }
    #endif

    for (; i < n_el; ++i)
        if (mask[i])
            rtn_v[out_idx++] = col_vec[i];
}



