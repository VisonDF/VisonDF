#pragma once

#if defined (__AVX512F__)

inline void float_to_u32_avx512(uint32_t* dst,
                                const float* src,
                                const size_t start,
                                const size_t end)
{
    size_t i = start;

    const __m512i signbit = _mm512_set1_epi32(0x80000000u);

    for (; i + 16 <= end; i += 16)
    {
        __m512  f    = _mm512_loadu_ps(src + i);

        __m512i bits = _mm512_castps_si512(f);

        __m512i signmask = _mm512_srai_epi32(bits, 31);

        __m512i flipmask = _mm512_or_si512(signmask, signbit);

        __m512i out = _mm512_xor_si512(bits, flipmask);

        _mm512_storeu_si512((__m512i*)(dst + i), out);
    }

    for (; i < end; ++i)
    {
        uint32_t bits;
        memcpy(&bits, &src[i], sizeof(uint32_t));
        uint32_t mask = -(bits >> 31);
        dst[i] = bits ^ (mask | 0x80000000u);
    }
}


#endif


