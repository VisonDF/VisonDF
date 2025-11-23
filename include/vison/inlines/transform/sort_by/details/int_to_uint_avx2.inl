#pragma once

#if defined (__AVX2__)

template <typename UIntT, typename IntT>
inline void int_to_uint_avx2(UIntT* dst,
                             const IntT* src,
                             size_t start,
                             size_t end,
                             UIntT c_val)
{

    constexpr size_t W = sizeof(UIntT);  
    constexpr size_t VECTOR_BYTES = 32;
    constexpr size_t ELEMENTS_PER_VEC = VECTOR_BYTES / W;
    
    size_t i = start;

    if constexpr (W == 1)
    {
        const __m256i mask = _mm256_set1_epi8(static_cast<char>(c_val));

        for (; i + ELEMENTS_PER_VEC <= end; i += ELEMENTS_PER_VEC)
        {
            __m256i v = _mm256_loadu_si256((__m256i const*)(src + i));
            v = _mm256_xor_si256(v, mask);
            _mm256_storeu_si256((__m256i*)(dst + i), v);
        }
    }
    else if constexpr (W == 2)
    {
        const __m256i mask = _mm256_set1_epi16((int16_t)c_val);

        for (; i + ELEMENTS_PER_VEC <= end; i += ELEMENTS_PER_VEC)
        {
            __m256i v = _mm256_loadu_si256((__m256i const*)(src + i));
            v = _mm256_xor_si256(v, mask);
            _mm256_storeu_si256((__m256i*)(dst + i), v);
        }
    }
    else if constexpr (W == 4)
    {
        const __m256i mask = _mm256_set1_epi32((int32_t)c_val);

        for (; i + ELEMENTS_PER_VEC <= end; i += ELEMENTS_PER_VEC)
        {
            __m256i v = _mm256_loadu_si256((__m256i const*)(src + i));
            v = _mm256_xor_si256(v, mask);
            _mm256_storeu_si256((__m256i*)(dst + i), v);
        }
    }
    else if constexpr (W == 8)
    {
        const __m256i mask = _mm256_set1_epi64x((long long)c_val);

        for (; i + ELEMENTS_PER_VEC <= end; i += ELEMENTS_PER_VEC)
        {
            __m256i v = _mm256_loadu_si256((__m256i const*)(src + i));
            v = _mm256_xor_si256(v, mask);
            _mm256_storeu_si256((__m256i*)(dst + i), v);
        }
    }
    else
    {
        static_assert(W == 1 || W == 2 || W == 4 || W == 8,
                      "Unsupported element size for AVX2");
    }

    for (; i < end; ++i)
        dst[i] = UIntT(src[i]) ^ c_val;
}

#endif


