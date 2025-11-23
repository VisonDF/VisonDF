#pragma once

#if defined (__AVX2__)

template <char PaddingChar = 0x00>
inline void string_to_u8buf_avx2(uint8_t* tkeys, 
                                 const std::string* __restrict col,
                                 const size_t max_length,
                                 const size_t start,
                                 const size_t end) {

    const uint8_t pad = uint8_t(PaddingChar) ^ 0x80u;
    const __m256i xor_mask = _mm256_set1_epi8(0x80);
    
    for (size_t i = start; i < end; ++i)
    {
        const std::string& s = col[i];
        const size_t len = s.size();
    
        uint8_t* dst = tkeys + i * max_length;
        const uint8_t* src = reinterpret_cast<const uint8_t*>(s.data());
    
        size_t j = 0;
    
        for (; j + 32 <= len; j += 32) {
            __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + j));
            v = _mm256_xor_si256(v, xor_mask);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + j), v);
        }
    
        for (; j < len; ++j)
            dst[j] = src[j] ^ 0x80u;
    
        std::memset(dst + len, pad, max_length - len);

    }

}

#endif


