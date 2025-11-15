#if defined(__AVX512F__)

inline void scatter_pass_u64_avx512(
    const uint64_t* tkeys,
    const size_t* idx,
    size_t n,
    size_t shift,
    size_t* count,
    size_t* tmp
)
{
    static_assert(sizeof(size_t) == 8, "AVX-512 scatter assumes 64-bit size_t");

    const __m512i mask16 = _mm512_set1_epi64(0xFFFFull);

    size_t i = 0;
    for (; i + 8 <= n; i += 8)
    {
        __m512i v = _mm512_loadu_si512(reinterpret_cast<const void*>(tkeys + i));
        __m512i buckets = _mm512_and_si512(
            _mm512_srli_epi64(v, shift),
            mask16
        );
        //ternary logic alternative 
        //__m512i buckets =
        //_mm512_ternarylogic_epi64(_mm512_srli_epi64(v, shift),
        //                           mask16,
        //                           _mm512_setzero_si512(),
        //                           0xA8);

        alignas(64) uint64_t b[8];
        _mm512_storeu_si512(b, buckets);

        // Build scatter offsets via prefix sums of count[b[k]]
        size_t offsets[8];
        for (int k = 0; k < 8; ++k)
            offsets[k] = count[b[k]]++;

        // Load indices to scatter
        __m512i idxv = _mm512_loadu_si512(reinterpret_cast<const void*>(idx + i));
        __m512i offv = _mm512_loadu_si512(reinterpret_cast<const void*>(offsets));

        // Scatter 8 indices
        _mm512_i64scatter_epi64(tmp, offv, idxv, 8);
    }

    // Remainder – scalar
    for (; i < n; ++i) {
        uint64_t b = (tkeys[i] >> shift) & 0xFFFF;
        tmp[count[b]++] = idx[i];
    }
}
#endif


