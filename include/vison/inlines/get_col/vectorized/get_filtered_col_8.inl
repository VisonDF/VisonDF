#pragma once

template <bool NeedsNormalization = true,
          typename T>
inline void get_filtered_col_8(
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
    for (; i + 64 <= end; i += 64)
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
        __m512i vals = _mm512_loadu_si512((const void*)&col_vec[strt_vl + i]);

        // Compress-store only active bytes
        _mm512_mask_compressstoreu_epi8(
            &rtn_v[out_idx],
            k,
            vals
        );

        out_idx += _mm_popcnt_u64(k);

    }
    #elif defined(__AVX2__)

    //constexpr auto LUT16 = make_LUT16(); // already defined inside avx_lut16.inl
    
    for (; i + 32 <= end; i += 32)
    {
        __m256i mbytes = _mm256_loadu_si256(
            reinterpret_cast<const __m256i*>(&mask[i])
        );

        uint32_t maskbits;
        if constexpr (NeedsNormalization) {
            __m256i nz = _mm256_cmpgt_epi8(mbytes, _mm256_setzero_si256()); 
            maskbits = static_cast<uint32_t>(_mm256_movemask_epi8(nz));
        } else {
            maskbits = static_cast<uint32_t>(_mm256_movemask_epi8(mbytes));
        }

        uint16_t m_lo = static_cast<uint16_t>( maskbits        & 0xFFFFu );
        uint16_t m_hi = static_cast<uint16_t>((maskbits >> 16) & 0xFFFFu );

        __m256i v = _mm256_loadu_si256(
            reinterpret_cast<const __m256i*>(&col_vec[strt_vl + i])
        );

        __m128i v0 = _mm256_castsi256_si128(v);                 
        __m128i v1 = _mm256_extracti128_si256(v, 1);            

        {
            const Lut16Entry &e = LUT16[m_lo];
            __m128i shuf = _mm_loadu_si128(reinterpret_cast<const __m128i*>(e.shuf));
            __m128i res  = _mm_shuffle_epi8(v0, shuf);

            _mm_storeu_si128(reinterpret_cast<__m128i*>(&rtn_v[out_idx]), res);
            out_idx += e.count;
        }

        {
            const Lut16Entry &e = LUT16[m_hi];
            __m128i shuf = _mm_loadu_si128(reinterpret_cast<const __m128i*>(e.shuf));
            __m128i res  = _mm_shuffle_epi8(v1, shuf);

            _mm_storeu_si128(reinterpret_cast<__m128i*>(&rtn_v[out_idx]), res);
            out_idx += e.count;
        }
    }
    #endif

    for (; i < end; ++i)
        if (mask[i])
            rtn_v[out_idx++] = col_vec[strt_vl + i];
}



