#if defined(__AVX512F__)

inline void scatter_pass_u8_avx512(
    const uint8_t* __restrict data,
    const size_t* __restrict idx,
    size_t n,
    size_t* __restrict count,   // 256 buckets
    size_t* __restrict tmp
)
{
    static_assert(sizeof(size_t) == 8, "AVX-512 scatter assumes 64-bit size_t");

    size_t i = 0;

    for (; i + 16 <= n; i += 16)
    {
        // Load 16 bytes → widen to 16×u32
        __m128i raw = _mm_loadu_si128(reinterpret_cast<const __m128i*>(data + i));
        __m512i buckets = _mm512_cvtepu8_epi32(raw);  // produces 16 lanes of 0..255

        // Store buckets to stack
        alignas(64) uint32_t b[16];
        _mm512_storeu_si512(reinterpret_cast<__m512i*>(b), buckets);

        // Compute scatter offsets = count[b[k]]++
        size_t offsets[16];
        for (int k = 0; k < 16; ++k)
            offsets[k] = count[b[k]]++;

        // Load idx values and offsets
        __m512i idxv = _mm512_loadu_si512(reinterpret_cast<const void*>(idx + i));
        __m512i offv = _mm512_loadu_si512(reinterpret_cast<const void*>(offsets));

        // Scatter each 64-bit idxv[k] into tmp[offsets[k]]
        _mm512_i64scatter_epi64(tmp, offv, idxv, 8);
    }

    // Tail → scalar
    for (; i < n; ++i) {
        uint32_t b = data[i];
        tmp[count[b]++] = idx[i];
    }
}

#endif


