#if defined(__AVX512F__)

inline void scatter_pass_u64_avx512(
    const uint64_t* tkeys,   // input keys
    const size_t*   idx,     // input indices
    size_t          n,       // number of items
    size_t          shift,   // digit shift
    size_t*         count,   // bucket offsets (must be per-thread in MT)
    size_t*         tmp_idx, // output idx array
    uint64_t*       tmp_keys // output key array
)
{
    static_assert(sizeof(size_t) == 8, "AVX-512 scatter assumes 64-bit size_t");

    const __m512i mask16 = _mm512_set1_epi64(0xFFFFull);

    size_t i = 0;
    for (; i + 8 <= n; i += 8)
    {
        // Load 8 keys
        __m512i v = _mm512_loadu_si512((const void*)(tkeys + i));

        // Compute 8 bucket indices (16-bit)
        __m512i buckets = _mm512_and_si512(
            _mm512_srli_epi64(v, shift),
            mask16
        );

        // Extract bucket numbers to scalar array
        alignas(64) uint64_t b[8];
        _mm512_storeu_si512(b, buckets);

        // Compute offsets: pos = count[b[k]]++
        alignas(64) uint64_t offsets[8];
        for (int k = 0; k < 8; ++k)
            offsets[k] = count[b[k]]++;

        // Load offsets and values
        __m512i offv = _mm512_loadu_si512((const void*)offsets);
        __m512i idxv = _mm512_loadu_si512((const void*)(idx + i));
        __m512i keyv = _mm512_loadu_si512((const void*)(tkeys + i));

        // Scatter indices
        _mm512_i64scatter_epi64(tmp_idx, offv, idxv, 8);

        // Scatter keys (also 64-bit)
        _mm512_i64scatter_epi64(tmp_keys, offv, keyv, 8);
    }

    // Scalar remainder
    for (; i < n; ++i) {
        uint64_t b = (tkeys[i] >> shift) & 0xFFFFull;
        size_t pos = count[b]++;
        tmp_idx[pos]  = idx[i];
        tmp_keys[pos] = tkeys[i];
    }
}

#endif


