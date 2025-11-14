#if defined(__AVX512F__)

inline void scatter_pass_u8_avx512_mt (
    const uint8_t* __restrict data,  // keys (optionally offset by beg)
    size_t* __restrict idx,          // global output permutation
    size_t base_index,               // starting logical index (0 for ST, beg for MT)
    size_t n,                        // number of items to process
    size_t* __restrict count         // bucket offsets (prefix sum), updated in place
)
{
    static_assert(sizeof(size_t) == 8, "AVX-512 scatter assumes 64-bit size_t");

    size_t i = 0;

    // process 16 keys per iteration
    for (; i + 16 <= n; i += 16)
    {
        __builtin_prefetch(data + i + 64, 0, 1);

        // Load 16 bytes → widen to 16×u32
        __m128i raw     = _mm_loadu_si128(reinterpret_cast<const __m128i*>(data + i));
        __m512i buckets = _mm512_cvtepu8_epi32(raw);  // 16 lanes of 0..255

        // Store bucket indices to stack
        alignas(64) uint32_t b[16];
        _mm512_storeu_si512(reinterpret_cast<void*>(b), buckets);

        // Compute scatter offsets = count[b[k]]++
        alignas(64) size_t offsets[16];
        for (int k = 0; k < 16; ++k)
            offsets[k] = count[b[k]]++;

        // Load offsets as 2× vectors of 8×u64
        __m512i off0 = _mm512_loadu_si512(reinterpret_cast<const void*>(offsets + 0)); // 0..7
        __m512i off1 = _mm512_loadu_si512(reinterpret_cast<const void*>(offsets + 8)); // 8..15

        // Build identity indices base_index + (i + lane)
        const __m512i inc = _mm512_setr_epi64(0,1,2,3,4,5,6,7);

        __m512i base0 = _mm512_set1_epi64(static_cast<long long>(base_index + i));
        __m512i base1 = _mm512_set1_epi64(static_cast<long long>(base_index + i + 8));

        __m512i idxv0 = _mm512_add_epi64(base0, inc); // base_index + i + [0..7]
        __m512i idxv1 = _mm512_add_epi64(base1, inc); // base_index + i + [8..15]

        // Scatter: idx[offsets[k]] = base_index + (i + lane)
        _mm512_i64scatter_epi64(idx, off0, idxv0, 8);
        _mm512_i64scatter_epi64(idx, off1, idxv1, 8);
    }

    // Scalar tail
    for (; i < n; ++i) {
        uint8_t k = data[i];
        idx[count[k]++] = base_index + i;
    }
}

#endif


