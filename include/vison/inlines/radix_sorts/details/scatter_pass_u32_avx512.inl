#if defined(__AVX512F__)

inline void scatter_pass_u32_avx512(
    const uint32_t* tkeys,
    const size_t* idx,
    size_t n,
    size_t shift,
    size_t* count,
    size_t* tmp
)
{
    static_assert(sizeof(size_t) == 8, "AVX-512 scatter assumes 64-bit size_t");

    const __m512i mask16 = _mm512_set1_epi32(0xFFFF);

    size_t i = 0;
    for (; i + 16 <= n; i += 16)
    {
        __m512i v = _mm512_loadu_si512(reinterpret_cast<const void*>(tkeys + i));
        __m512i buckets = _mm512_and_si512(
            _mm512_srli_epi32(v, shift),
            mask16
        );

        alignas(64) uint32_t b[16];
        _mm512_storeu_si512(reinterpret_cast<__m512i*>(b), buckets);

        // Build scatter offsets via prefix sums of count[b[k]]
        size_t offsets[16];
        for (int k = 0; k < 16; ++k)
            offsets[k] = count[b[k]]++;

        // Load indices to scatter
        __m512i idxv = _mm512_loadu_si512(reinterpret_cast<const void*>(idx + i));
        __m512i offv = _mm512_loadu_si512(reinterpret_cast<const void*>(offsets));

        // Scatter 16 indices
        _mm512_i64scatter_epi64(tmp, offv, idxv, 8);
    }

    // Remainder – scalar
    for (; i < n; ++i) {
        uint32_t b = (tkeys[i] >> shift) & 0xFFFF;
        tmp[count[b]++] = idx[i];
    }
}
#endif


