#pragma once

#if defined (__AVX512F__)

inline void char_to_u8buf2d_avx512(
    uint8_t (* __restrict tkeys)[df_charbuf_size],
    const int8_t (* __restrict col)[df_charbuf_size],
    const size_t start,
    const size_t end,
    const size_t df_charbuf_size)
{

    constexpr size_t VecBytes = 64;
    const __m512i xor_mask = _mm512_set1_epi8(0x80);

    constexpr size_t NVEC     = df_charbuf_size / VecBytes;

    for (size_t i = start; i < end; ++i) {
        uint8_t*       dst = tkeys[i];
        const int8_t*  src = col[i];

        #pragma unroll
        for (size_t b = 0; b < NVEC; ++b) {
            const __m512i v =
                _mm512_loadu_si512(reinterpret_cast<const void*>(src + b * VecBytes));
            const __m512i x = _mm512_xor_si512(v, xor_mask);
            _mm512_storeu_si512(reinterpret_cast<void*>(dst + b * VecBytes), x);
        }

        #pragma unroll
        for (size_t j = NVEC * VecBytes; j < df_charbuf_size; ++j) {
            dst[j] = uint8_t(src[j]) ^ 0x80u;
        }
    }

}

#endif


