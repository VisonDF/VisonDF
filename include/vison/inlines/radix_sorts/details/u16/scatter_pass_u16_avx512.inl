#if defined(__AVX512F__)

inline void scatter_pass_u16_avx512(
    const uint16_t* __restrict data, // keys
    size_t* __restrict idx,          // output permutation
    size_t n,
    size_t* __restrict count         // 65536 buckets, prefix sum, updated in place
)
{
    static_assert(sizeof(size_t) == 8,
                  "AVX-512 scatter assumes 64-bit size_t");

    size_t i = 0;

    // Process 32 keys per iteration
    for (; i + 32 <= n; i += 32)
    {
        __builtin_prefetch(data + i + 64, 0, 1);

        // Load 32 × uint16
        __m512i raw = _mm512_loadu_si512(reinterpret_cast<const void*>(data + i));

        // Split into 2× 16×u16 halves
        __m256i low16  = _mm512_castsi512_si256(raw);               // elements 0..15
        __m256i high16 = _mm512_extracti64x4_epi64(raw, 1);         // elements 16..31

        // Widen to 32-bit buckets
        __m512i buckets_lo = _mm512_cvtepu16_epi32(low16);
        __m512i buckets_hi = _mm512_cvtepu16_epi32(high16);

        // Dump 32 bucket indices to stack as u32
        alignas(64) uint32_t b[32];
        _mm512_storeu_si512(reinterpret_cast<void*>(b),        buckets_lo);
        _mm512_storeu_si512(reinterpret_cast<void*>(b + 16),   buckets_hi);

        // Compute scatter offsets = count[b[k]]++
        alignas(64) size_t offsets[32];
        for (int k = 0; k < 32; ++k)
            offsets[k] = count[b[k]]++;

        // Load offsets (8×u64 per vector)
        __m512i off0 = _mm512_loadu_si512(reinterpret_cast<const void*>(offsets +  0)); // 0..7
        __m512i off1 = _mm512_loadu_si512(reinterpret_cast<const void*>(offsets +  8)); // 8..15
        __m512i off2 = _mm512_loadu_si512(reinterpret_cast<const void*>(offsets + 16)); // 16..23
        __m512i off3 = _mm512_loadu_si512(reinterpret_cast<const void*>(offsets + 24)); // 24..31

        // Build identity indices i..i+31 as 4 vectors of 8×u64
        const __m512i inc = _mm512_setr_epi64(0,1,2,3,4,5,6,7);
        __m512i base0 = _mm512_set1_epi64(static_cast<long long>(i));          // i
        __m512i base1 = _mm512_add_epi64(base0, _mm512_set1_epi64(8));        // i+8
        __m512i base2 = _mm512_add_epi64(base0, _mm512_set1_epi64(16));       // i+16
        __m512i base3 = _mm512_add_epi64(base0, _mm512_set1_epi64(24));       // i+24

        __m512i idxv0 = _mm512_add_epi64(base0, inc); // i+0..7
        __m512i idxv1 = _mm512_add_epi64(base1, inc); // i+8..15
        __m512i idxv2 = _mm512_add_epi64(base2, inc); // i+16..23
        __m512i idxv3 = _mm512_add_epi64(base3, inc); // i+24..31

        // Scatter: idx[offsets[k]] = i+k  (in parallel)
        _mm512_i64scatter_epi64(idx, off0, idxv0, 8);
        _mm512_i64scatter_epi64(idx, off1, idxv1, 8);
        _mm512_i64scatter_epi64(idx, off2, idxv2, 8);
        _mm512_i64scatter_epi64(idx, off3, idxv3, 8);
    }

    // Scalar tail
    for (; i < n; ++i) {
        uint16_t k = data[i];
        idx[count[k]++] = i;
    }
}

#endif


