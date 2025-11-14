#if defined(__AVX512F__)

inline void scatter_pass_u32_avx512_mt(
    const uint32_t* __restrict tkeys,   // keys + beg
    const size_t*  __restrict idx_in,   // idx + beg (input permutation)
    size_t* __restrict tmp,             // global output permutation
    size_t n,                           // len = end - beg
    size_t shift,                       // 0 or 16
    size_t* __restrict count            // per-thread bucket offsets
)
{
    static_assert(sizeof(size_t) == 8, "AVX-512 scatter assumes 64-bit size_t");

    const __m512i mask16 = _mm512_set1_epi32(0xFFFF);

    size_t i = 0;

    for (; i + 16 <= n; i += 16)
    {
        __builtin_prefetch(tkeys + i + 128, 0, 1);

        // Load 16 keys
        __m512i v = _mm512_loadu_si512(reinterpret_cast<const void*>(tkeys + i));

        // Extract 16-bit bucket indices
        __m512i buckets = _mm512_and_si512(
            _mm512_srli_epi32(v, shift),
            mask16
        );

        alignas(64) uint32_t b[16];
        _mm512_store_si512(b, buckets);

        // Compute scatter offsets = count[b[k]]++
        alignas(64) size_t offsets[16];
        for (int k = 0; k < 16; ++k)
            offsets[k] = count[b[k]]++;

        // Load 16 input indices (size_t) in two 8-lane chunks
        __m512i idxv0 = _mm512_loadu_si512(reinterpret_cast<const void*>(idx_in + i));
        __m512i idxv1 = _mm512_loadu_si512(reinterpret_cast<const void*>(idx_in + i + 8));

        // Load offsets as 2 × 8-lane vectors
        __m512i off0 = _mm512_loadu_si512(reinterpret_cast<const void*>(offsets + 0)); // 0..7
        __m512i off1 = _mm512_loadu_si512(reinterpret_cast<const void*>(offsets + 8)); // 8..15

        // Scatter: tmp[offsets[k]] = idx_in[i+k]
        _mm512_i64scatter_epi64(tmp, off0, idxv0, 8);
        _mm512_i64scatter_epi64(tmp, off1, idxv1, 8);
    }

    // Scalar tail
    for (; i < n; ++i) {
        uint32_t bucket = (tkeys[i] >> shift) & 0xFFFFu;
        tmp[count[bucket]++] = idx_in[i];
    }
}

#endif


