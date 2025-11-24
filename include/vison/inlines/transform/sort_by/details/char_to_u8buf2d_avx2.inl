#pragma once

#if defined (__AVX2__)

inline void char_to_u8buf2d_avx2(
    uint8_t (* __restrict tkeys)[df_charbuf_size],
    const int8_t (* __restrict col)[df_charbuf_size],
    const size_t start,
    const size_t end,
    const size_t df_charbuf_size)
{

    const __m256i xor_mask = _mm256_set1_epi8(0x80);
    constexpr size_t VecBytes = 32;

    constexpr size_t NVEC = df_charbuf_size / VecBytes;

    for (size_t i = start; i < end; ++i) {

        uint8_t* dst = tkeys[i];
        const int8_t* src = col[i];

        #pragma unroll
        for (size_t b = 0; b < NVEC; ++b) {
            const __m256i v =
                _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + b * VecBytes));
            const __m256i x = _mm256_xor_si256(v, xor_mask);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + b * VecBytes), x);
        }

        #pragma unroll
        for (size_t j = NVEC * VecBytes; j < df_charbuf_size; ++j) {
            dst[j] = uint8_t(src[j]) ^ 0x80u;
        }
    }

}

#endif

