#pragma once

inline void double_to_u64_avx2(uint64_t* dst,
                               const double* src,
                               const size_t start,
                               const size_t end)
{
    size_t i = start;

    const __m256i signbit = _mm256_set1_epi64x(0x8000000000000000ULL);

    for (; i + 4 <= end; i += 4)
    {
        // Load 4 doubles
        __m256d f = _mm256_loadu_pd(src + i);

        // Reinterpret as uint64_t lanes
        __m256i bits = _mm256_castpd_si256(f);

       // Extract the sign bit for each 64-bit lane.
       // AVX2 has no arithmetic right shift for 64-bit integers 
       // (_mm256_srai_epi64 does not exist),
       // so we emulate it:
       //
       // Step 1: Logical shift right by 63 bits.
       //         This yields either 0 (if the double is positive) or 1 (if negative)
       //         in each 64-bit lane.
       //             tmp = bits >> 63   →   [0 or 1]
       //
       // Step 2: Convert 0/1 into a full 64-bit mask.
       //         We subtract tmp from zero:
       //             0 - 0 = 0x0000000000000000
       //             0 - 1 = 0xFFFFFFFFFFFFFFFF
       //
       //         This works because -1 in two’s-complement is an all-ones mask.
       //         The subtraction operates independently on each 64-bit lane,
       //         since _mm256_setzero_si256() provides four u64 zeros in a 256-bit vector.
       //
       // Result: signmask is either all-zero or all-ones per lane,
       //         exactly the mask we need.
       //
        __m256i tmp = _mm256_srli_epi64(bits, 63);
        __m256i signmask = _mm256_sub_epi64(_mm256_setzero_si256(), tmp);

        // OR with signbit
        __m256i flipmask = _mm256_or_si256(signmask, signbit);

        // XOR to produce sortable key
        __m256i out = _mm256_xor_si256(bits, flipmask);

        _mm256_storeu_si256((__m256i*)(dst + i), out);
    }

    // Scalar tail
    for (; i < end; ++i)
    {
        uint64_t bits;
        memcpy(&bits, &src[i], sizeof(bits));
        uint64_t mask = -(bits >> 63);
        dst[i] = bits ^ (mask | 0x8000000000000000ULL);
    }
}


