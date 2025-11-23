#pragma once

inline void double_to_u64_avx512(uint64_t* dst,
                                 const double* src,
                                 const size_t start,
                                 const size_t end)
{
    size_t i = start;

    const __m512i signbit = _mm512_set1_epi64(0x8000000000000000ULL);

    for (; i + 8 <= end; i += 8)
    {
        __m512d f    = _mm512_loadu_pd(src + i);

        // Reinterpret as 8×uint64_t (no conversion, just bit reinterpretation)
        __m512i bits = _mm512_castpd_si512(f);

        // Arithmetic shift right by 63 bits to get the sign mask per lane:
        //   >= 0  → 0x0000000000000000
        //   < 0   → 0xFFFFFFFFFFFFFFFF
        __m512i signmask = _mm512_srai_epi64(bits, 63);

        // flipmask = signmask | signbit
        //   >= 0 → 0x8000...
        //   < 0  → 0xFFFF...
        __m512i flipmask = _mm512_or_si512(signmask, signbit);

        // out = bits ^ flipmask
        __m512i out = _mm512_xor_si512(bits, flipmask);

        // Store 8 uint64_t results
        _mm512_storeu_si512((__m512i*)(dst + i), out);
    }

    for (; i < end; ++i)
    {
        uint64_t bits;
        std::memcpy(&bits, &src[i], sizeof(bits));

        uint64_t mask = -(bits >> 63);  // 0 or 0xFFFF...FFFF
        dst[i] = bits ^ (mask | 0x8000000000000000ULL);
    }
}


