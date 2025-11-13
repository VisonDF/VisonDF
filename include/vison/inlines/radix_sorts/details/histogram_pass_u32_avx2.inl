#pragma once

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
#endif




