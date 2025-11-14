#if defined(__AVX512F__)

inline void scatter_pass_u16_avx512_mt(
    const uint16_t* __restrict data, // local keys (keys + beg)
    size_t* __restrict idx,          // global output permutation
    size_t base_index,               // global offset = beg
    size_t n,                        // local length
    size_t* __restrict count         // thread-local prefix offsets
)
{
    static_assert(sizeof(size_t) == 8,
                  "AVX-512 scatter assumes 64-bit size_t");

    size_t i = 0;

    // Process 32 keys per loop (AVX-512 friendly)
    for (; i + 32 <= n; i += 32)
    {
        __builtin_prefetch(data + i + 64, 0, 1);

        // Load 32 × u16
        __m512i raw = _mm512_loadu_si512((const void*)(data + i));

        // Split: low 16, high 16
        __m256i low16  = _mm512_extracti64x4_epi64(raw, 0);  // 0..15
        __m256i high16 = _mm512_extracti64x4_epi64(raw, 1);  // 16..31

        // Convert to 32-bit buckets
        __m512i buckets_lo = _mm512_cvtepu16_epi32(low16);
        __m512i buckets_hi = _mm512_cvtepu16_epi32(high16);

        // Store bucket indices to stack
        alignas(64) uint32_t b[32];
        _mm512_storeu_si512(b,       buckets_lo);
        _mm512_storeu_si512(b + 16,  buckets_hi);

        // Compute offsets = count[b[k]]++
        alignas(64) size_t offsets[32];
        for (int k = 0; k < 32; k++)
            offsets[k] = count[b[k]]++;

        // Load offsets in 4 chunks of 8×u64
        __m512i off0 = _mm512_loadu_si512((const void*)(offsets +  0));
        __m512i off1 = _mm512_loadu_si512((const void*)(offsets +  8));
        __m512i off2 = _mm512_loadu_si512((const void*)(offsets + 16));
        __m512i off3 = _mm512_loadu_si512((const void*)(offsets + 24));

        // Build identity indices (global)
        __m512i inc   = _mm512_setr_epi64(0,1,2,3,4,5,6,7);
        __m512i base0 = _mm512_set1_epi64((long long)(base_index + i +  0));
        __m512i base1 = _mm512_set1_epi64((long long)(base_index + i +  8));
        __m512i base2 = _mm512_set1_epi64((long long)(base_index + i + 16));
        __m512i base3 = _mm512_set1_epi64((long long)(base_index + i + 24));

        __m512i idxv0 = _mm512_add_epi64(base0, inc);
        __m512i idxv1 = _mm512_add_epi64(base1, inc);
        __m512i idxv2 = _mm512_add_epi64(base2, inc);
        __m512i idxv3 = _mm512_add_epi64(base3, inc);

        // Scatter into global idx
        _mm512_i64scatter_epi64(idx, off0, idxv0, 8);
        _mm512_i64scatter_epi64(idx, off1, idxv1, 8);
        _mm512_i64scatter_epi64(idx, off2, idxv2, 8);
        _mm512_i64scatter_epi64(idx, off3, idxv3, 8);
    }

    // Tail
    for (; i < n; i++) {
        uint16_t k = data[i];
        idx[count[k]++] = base_index + i;
    }
}

#endif



