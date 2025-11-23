#pragma once

#if defined (__AVX512F__)

template <char PaddingChar = 0x00>
inline void string_to_u8buf_avx512(
    uint8_t* __restrict tkeys,
    const std::string* __restrict col,
    size_t max_length,
    size_t start,
    size_t end
)
{
    const uint8_t pad = uint8_t(PaddingChar) ^ 0x80u;
    const __m512i xor_mask = _mm512_set1_epi8(0x80);

    for (size_t i = start; i < end; ++i)
    {
        const std::string& s = col[i];
        const size_t len = s.size();

        uint8_t* dst = tkeys + i * max_length;
        const uint8_t* src = reinterpret_cast<const uint8_t*>(s.data());

        size_t j = 0;

        for (; j + 64 <= len; j += 64) {
            __m512i v = _mm512_loadu_si512(reinterpret_cast<const void*>(src + j));
            v = _mm512_xor_si512(v, xor_mask);
            _mm512_storeu_si512(reinterpret_cast<void*>(dst + j), v);
        }

        for (; j < len; ++j)
            dst[j] = src[j] ^ 0x80u;

        std::memset(dst + len, pad, max_length - len);
    }
}

#endif



