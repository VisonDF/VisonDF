#if defined(__AVX512F__)

inline void scatter_pass_u32_avx512(
    const uint32_t* tkeys,     // current sortable keys
    const size_t*   idx,       // current permutation
    size_t          n,
    size_t          shift,
    size_t*         count,     // bucket prefix sums
    size_t*         tmp_idx,   // output idx buffer
    uint32_t*       tmp_keys   // output keys buffer
)
{
    static_assert(sizeof(size_t) == 8, "AVX-512 scatter assumes 64-bit size_t");

    const __m512i mask16 = _mm512_set1_epi32(0xFFFF);

    size_t i = 0;
    for (; i + 16 <= n; i += 16)
    {
        // Load 16 keys
        __m512i v = _mm512_loadu_si512(tkeys + i);

        // Compute 16 bucket numbers (16-bit)
        __m512i buckets = _mm512_and_si512(
            _mm512_srli_epi32(v, shift),
            mask16
        );

        // Store buckets temporarily
        alignas(64) uint32_t b[16];
        _mm512_storeu_si512(b, buckets);

        // Compute output offsets via count[b[k]]++
        alignas(64) size_t offsets[16];
        for (int k = 0; k < 16; ++k)
            offsets[k] = count[b[k]]++;

        // Load offsets split as 8 + 8 (because scatter uses 8×64-bit lanes)
        __m512i off0 = _mm512_loadu_si512(offsets);
        __m512i off1 = _mm512_loadu_si512(offsets + 8);

        // Load 16 idx
        __m512i idx0 = _mm512_loadu_si512(idx + i);
        __m512i idx1 = _mm512_loadu_si512(idx + i + 8);

        // Scatter idx
        _mm512_i64scatter_epi64(tmp_idx, off0, idx0, 8);
        _mm512_i64scatter_epi64(tmp_idx, off1, idx1, 8);

        // Load 16 keys (again or reuse v)
        __m512i key0 = _mm512_loadu_si512(tkeys + i);
        __m512i key1 = _mm512_loadu_si512(tkeys + i + 8);

        // Scatter keys (32-bit each, stored into uint32_t array)
        _mm512_i32scatter_epi32(tmp_keys, off0, key0, 4);
        _mm512_i32scatter_epi32(tmp_keys, off1, key1, 4);
    }

    // Remainder – scalar fallback
    for (; i < n; ++i) {
        uint32_t key = tkeys[i];
        uint32_t b   = (key >> shift) & 0xFFFF;
        size_t   pos = count[b]++;

        tmp_idx[pos]  = idx[i];
        tmp_keys[pos] = key;
    }
}

#endif



