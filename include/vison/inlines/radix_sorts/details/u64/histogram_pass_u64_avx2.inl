#pragma once
#if defined(__AVX2__)

inline void histogram_pass_u64_avx2(
    const uint64_t* __restrict tkeys,
    size_t n,
    size_t shift,
    size_t* __restrict count   // K = 65536 buckets
)
{
    const __m256i mask = _mm256_set1_epi64x(0xFFFFull);

    size_t i = 0;

    // 4× unrolled loop: process 16 × u64 per iteration
    for (; i + 16 <= n; i += 16)
    {
        __builtin_prefetch(tkeys + i + 64, 0, 1);

        // -------- LOAD 4× blocks of 4×u64 = 16 keys --------
        __m256i v0 = _mm256_loadu_si256((const __m256i*)(tkeys + i +  0));
        __m256i v1 = _mm256_loadu_si256((const __m256i*)(tkeys + i +  4));
        __m256i v2 = _mm256_loadu_si256((const __m256i*)(tkeys + i +  8));
        __m256i v3 = _mm256_loadu_si256((const __m256i*)(tkeys + i + 12));

        // -------- SHIFT --------
        __m256i s0 = _mm256_srli_epi64(v0, shift);
        __m256i s1 = _mm256_srli_epi64(v1, shift);
        __m256i s2 = _mm256_srli_epi64(v2, shift);
        __m256i s3 = _mm256_srli_epi64(v3, shift);

        // -------- AND with mask --------
        __m256i b0 = _mm256_and_si256(s0, mask);
        __m256i b1 = _mm256_and_si256(s1, mask);
        __m256i b2 = _mm256_and_si256(s2, mask);
        __m256i b3 = _mm256_and_si256(s3, mask);

        // -------- Extract 16 bucket indices --------
        alignas(32) uint64_t buf[16];
        _mm256_store_si256((__m256i*)(buf +  0), b0);
        _mm256_store_si256((__m256i*)(buf +  4), b1);
        _mm256_store_si256((__m256i*)(buf +  8), b2);
        _mm256_store_si256((__m256i*)(buf + 12), b3);

        // -------- Increment 16 buckets --------
        count[buf[ 0]]++; count[buf[ 1]]++;
        count[buf[ 2]]++; count[buf[ 3]]++;
        count[buf[ 4]]++; count[buf[ 5]]++;
        count[buf[ 6]]++; count[buf[ 7]]++;
        count[buf[ 8]]++; count[buf[ 9]]++;
        count[buf[10]]++; count[buf[11]]++;
        count[buf[12]]++; count[buf[13]]++;
        count[buf[14]]++; count[buf[15]]++;
    }

    // Tail
    for (; i < n; ++i)
    {
        uint64_t bucket = (tkeys[i] >> shift) & 0xFFFFu;
        count[bucket]++;
    }
}

#endif

