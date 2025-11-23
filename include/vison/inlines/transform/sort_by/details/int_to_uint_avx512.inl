#pragma once

inline void int_to_uint_avx512(UIntT* dst, 
                               const IntT* src, 
                               const size_t start,
                               const size_t end,
                               const uint8_t c_byte)
{
    const __m512i mask = _mm512_set1_epi8(static_cast<char>(c_byte));

    size_t i = start;
    for (; i + 64 <= end; i += 64) {
        __m512i v = _mm512_loadu_si512((__m512i const*)(src + i));
        v = _mm512_xor_si512(v, mask);
        _mm512_storeu_si512((__m512i*)(dst + i), v);
    }

    for (; i < end; ++i)
        dst[i] = uint8_t(src[i]) ^ c_byte;

}
