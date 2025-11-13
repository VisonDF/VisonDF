#pragma once

constexpr size_t RADIX_KI32 = 1u << 16;        
constexpr size_t RADIX_LANES = 8;
constexpr size_t RADIX_LANES_AVX512 = 16;

#if defined(__AVX2__)
inline void histogram_pass_u32_avx2(
    const uint32_t* tkeys,
    size_t n,
    size_t shift,
    size_t* count   // K = 65536 buckets
)
{
    const __m256i mask = _mm256_set1_epi32(0xFFFF);

    size_t i = 0;

    // Process 8 keys per iteration
    for (; i + 8 <= n; i += 8) {
        // Load 8 x uint32_t
        __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(tkeys + i));

        // Shift right by 'shift' bits, keep only 16 bits
        __m256i buckets = _mm256_and_si256(_mm256_srli_epi32(v, shift), mask);

        // Extract the 8 bucket indices to scalar array
        alignas(32) uint32_t b[8];
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(b), buckets);

        // Increment 8 buckets
        count[b[0]]++;
        count[b[1]]++;
        count[b[2]]++;
        count[b[3]]++;
        count[b[4]]++;
        count[b[5]]++;
        count[b[6]]++;
        count[b[7]]++;
    }

    // Scalar tail for remaining elements
    for (; i < n; ++i) {
        uint32_t bucket = (tkeys[i] >> shift) & 0xFFFFu;
        count[bucket]++;
    }
}

inline size_t* get_local_histogram_8x() {
    static thread_local std::array<size_t, RADIX_LANES * RADIX_KI32> local{};
    return local.data();
}

inline void histogram_pass_u32_avx2_8buckets(
    const uint32_t* __restrict tkeys,
    size_t n,
    size_t shift,
    size_t* __restrict count
)
{
    const __m256i mask = _mm256_set1_epi32(0xFFFF);

    size_t* __restrict local = get_local_histogram_8x();
    memset(local, 0, RADIX_LANES * RADIX_KI32 * sizeof(size_t));

    size_t* lanes[RADIX_LANES] = {
        local + 0 * RADIX_KI32,
        local + 1 * RADIX_KI32,
        local + 2 * RADIX_KI32,
        local + 3 * RADIX_KI32,
        local + 4 * RADIX_KI32,
        local + 5 * RADIX_KI32,
        local + 6 * RADIX_KI32,
        local + 7 * RADIX_KI32
    };

    size_t i = 0;

    for (; i + 8 <= n; i += 8)
    {
        __builtin_prefetch(tkeys + i + 64, 0, 1);

        __m256i v = _mm256_loadu_si256((__m256i const*)(tkeys + i));
        __m256i buckets = _mm256_and_si256(_mm256_srli_epi32(v, shift), mask);

        alignas(32) uint32_t b[8];
        _mm256_storeu_si256((__m256i*)b, buckets);

        #pragma unroll(8)
        for (int lane = 0; lane < 8; lane++)
            lanes[lane][b[lane]]++;
    }

    // Scalar tail
    for (; i < n; i++) {
        uint32_t bucket = (tkeys[i] >> shift) & 0xFFFFu;
        lanes[0][bucket]++;
    }

    // Reduction
    for (size_t b = 0; b < RADIX_KI32; b++) {
        size_t s =
            lanes[0][b] +
            lanes[1][b] +
            lanes[2][b] +
            lanes[3][b] +
            lanes[4][b] +
            lanes[5][b] +
            lanes[6][b] +
            lanes[7][b];

        count[b] = s;
    }
}

#endif

#if defined(__AVX512F__)

inline size_t* get_local_histogram_16x() {
    static thread_local std::array<size_t, RADIX_LANES_AVX512 * RADIX_KI32> local{};
    return local.data();
}

inline void histogram_pass_u32_avx512_16buckets(
    const uint32_t* __restrict tkeys,
    size_t n,
    size_t shift,
    size_t* __restrict count   // K = 65536 buckets
)
{
    const __m512i mask = _mm512_set1_epi32(0xFFFF);

    // 16 independent histograms: [lane][bucket]
    size_t* __restrict local = get_local_histogram_16x();
    memset(local, 0, RADIX_LANES_AVX512 * RADIX_KI32 * sizeof(size_t));

    // Precompute lane pointers: lanes[lane][bucket]
    size_t* lanes[RADIX_LANES_AVX512];
    for (size_t lane = 0; lane < RADIX_LANES_AVX512; ++lane) {
        lanes[lane] = local + lane * RADIX_KI32;
    }

    size_t i = 0;

    // SIMD main loop: process 16 keys per iteration
    for (; i + 16 <= n; i += 16) {
        // optional prefetch ahead
        __builtin_prefetch(tkeys + i + 64, 0, 1);

        __m512i v = _mm512_loadu_si512(
            reinterpret_cast<const void*>(tkeys + i)
        );

        // shift and mask → 16 bucket indices in [0, 65535]
        __m512i buckets = _mm512_and_si512(
            _mm512_srli_epi32(v, shift),
            mask
        );

        // store lanes to stack
        alignas(64) uint32_t b[16];
        _mm512_storeu_si512(reinterpret_cast<void*>(b), buckets);

        // increment 16 lane histograms
        #pragma unroll(16)
        for (int lane = 0; lane < 16; ++lane) {
            lanes[lane][b[lane]]++;
        }
    }

    // Scalar tail → just use lane 0
    for (; i < n; ++i) {
        const uint32_t bucket = (tkeys[i] >> shift) & 0xFFFFu;
        lanes[0][bucket]++;
    }

    // Reduce 16 lane histograms into final count[]
    for (size_t b = 0; b < RADIX_KI32; ++b) {
        size_t s = 0;
        // unroll manually for max throughput
        s += lanes[0][b];
        s += lanes[1][b];
        s += lanes[2][b];
        s += lanes[3][b];
        s += lanes[4][b];
        s += lanes[5][b];
        s += lanes[6][b];
        s += lanes[7][b];
        s += lanes[8][b];
        s += lanes[9][b];
        s += lanes[10][b];
        s += lanes[11][b];
        s += lanes[12][b];
        s += lanes[13][b];
        s += lanes[14][b];
        s += lanes[15][b];

        count[b] = s;
    }
}

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

template <bool Simd = true>
inline void radix_sort_int32(const int32_t* keys,
                             size_t* idx,
                             size_t n)
{

    using U = uint32_t;
    constexpr size_t K = 1 << 16;     
    constexpr size_t PASSES = 2;      

    std::vector<U> tkeys(n);
    std::vector<size_t> tmp(n);
    std::vector<size_t> count(K);

    // signed → unsigned transform
    for (size_t i = 0; i < n; i++)
        tkeys[i] = U(keys[idx[i]]) ^ 0x80000000u;

    // 2 passes, each processing 16 bits of the 32-bit key.
    // pass = 0 → least significant 16 bits
    // pass = 1 → most significant 16 bits
    for (size_t pass = 0; pass < PASSES; pass++)
    {
        size_t shift = pass * 16;
                                                     
        // Extract the 16-bit digit for this pass.
        // The bucket index b is the value of this digit:
        //   - small b → smaller part of the key
        //   - large b → larger part of the key

        if constexpr (Simd) {
        
        #if defined(__AVX512F__)
            histogram_pass_u32_avx512_16buckets(tkeys.data(), n, shift, count.data());
            
        #elif defined(__AVX2__)
            if (n < 200'000) {
                memset(count.data(), 0, K * sizeof(size_t));
                histogram_pass_u32_avx2(tkeys.data(), n, shift, count.data());
            } else {
                histogram_pass_u32_avx2_8buckets(tkeys.data(), n, shift, count.data());
            }
        #endif
        
        } else {
            memset(count.data(), 0, K * sizeof(size_t));
            for (size_t i = 0; i < n; ++i)
                count[(tkeys[i] >> shift) & 0xFFFF]++;
        }
                                                   
        // Convert histogram to prefix sums:
        // count[b] becomes the starting output index for bucket b.
        size_t sum = 0;
        for (size_t i = 0; i < K; i++) {
            size_t c = count[i];
            count[i] = sum;
            sum += c;
        }

        // Stable scatter:
        // Place each index into its correct bucket position in tmp[].
        #if defined(__AVX512F__)
            if constexpr (Simd) {
                scatter_pass_u32_avx512(tkeys.data(), 
                                        idx, 
                                        n, 
                                        shift, 
                                        count.data(), 
                                        tmp.data());
            } else
        #endif
            {
                for (size_t i = 0; i < n; i++) {
                    U key = tkeys[i];
                    size_t b = (key >> shift) & 0xFFFF; // same index than for 
                                                        // the histogram construction
                    tmp[count[b]++] = idx[i]; 
                }
            }

        memcpy(idx, tmp.data(), n * sizeof(size_t));

        // Rebuild transformed keys in the new order for the next pass.
        for (size_t i = 0; i < n; i++)
            tkeys[i] = (uint32_t(keys[idx[i]]) ^ 0x80000000u);
    }
}




