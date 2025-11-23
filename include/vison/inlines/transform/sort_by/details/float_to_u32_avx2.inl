#pragma once

#if defined (__AVX2__)

inline void float_to_u32_avx2(uint32_t* dst,
                              const float* src,
                              const size_t start,
                              const size_t end)
{
    size_t i = start;

    const __m256i signbit = _mm256_set1_epi32(0x80000000u);

    for (; i + 8 <= end; i += 8)
    {
        __m256 f = _mm256_loadu_ps(src + i);

        __m256i bits = _mm256_castps_si256(f);

        __m256i signmask = _mm256_srai_epi32(bits, 31);

        __m256i flipmask = _mm256_or_si256(signmask, signbit);

        __m256i out = _mm256_xor_si256(bits, flipmask);

        _mm256_storeu_si256((__m256i*)(dst + i), out);
    }

    for (; i < end; i++)
    {
        uint32_t bits;
        memcpy(&bits, &src[i], sizeof(uint32_t));
        uint32_t mask = -(bits >> 31);
        dst[i] = bits ^ (mask | 0x80000000u);
    }
}

#endif

