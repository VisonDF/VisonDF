#pragma once

inline void int_to_uint_avx2(UIntT* dst, 
                             const IntT* src, 
                             const size_t start,
                             const size_t end,
                             const uint8_t c_byte)
{
    const __m256i mask = _mm256_set1_epi8(static_cast<char>(c_byte));

    size_t i = start;
    for (; i + 32 <= end; i += 32) {
        __m256i v = _mm256_loadu_si256((__m256i const*)(src + i));
        v = _mm256_xor_si256(v, mask);
        _mm256_storeu_si256((__m256i*)(dst + i), v);
    }

    for (; i < end; ++i)
        dst[i] = uint8_t(src[i]) ^ c_byte;

}
