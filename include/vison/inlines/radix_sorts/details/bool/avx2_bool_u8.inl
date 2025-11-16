#pragma once

inline void avx2_bool_u8(const uint8_t* keys,
                                   size_t n,
                                   size_t* idx)
{
    size_t lo = 0;
    size_t hi = n;

    size_t i = 0;

    const __m256i zero = _mm256_setzero_si256();

    while (i + 32 <= n) {
        __m256i v = _mm256_loadu_si256((const __m256i*)(keys + i));

        // Compare each byte with zero: 0 -> 0xFF, nonzero -> 0x00
        __m256i cmp = _mm256_cmpeq_epi8(v, zero);

        // movemask puts the top bits of each byte into a 32-bit integer
        // cmp == 0? --> means key != 0 --> true bit

        uint32_t false_mask = _mm256_movemask_epi8(cmp);
        
        // Scatter 32 indices according to the mask
        #define HANDLE(J) do { \
            size_t logical = i + (J); \
            if (false_mask & (1u << (J))) \
                idx[lo++] = logical; \
            else \
                idx[--hi] = logical; \
        } while(0)
       
        HANDLE(0);  HANDLE(1);  HANDLE(2);  HANDLE(3);
        HANDLE(4);  HANDLE(5);  HANDLE(6);  HANDLE(7);
        HANDLE(8);  HANDLE(9);  HANDLE(10); HANDLE(11);
        HANDLE(12); HANDLE(13); HANDLE(14); HANDLE(15);
        HANDLE(16); HANDLE(17); HANDLE(18); HANDLE(19);
        HANDLE(20); HANDLE(21); HANDLE(22); HANDLE(23);
        HANDLE(24); HANDLE(25); HANDLE(26); HANDLE(27);
        HANDLE(28); HANDLE(29); HANDLE(30); HANDLE(31);

        i += 32;
    }

    // scalar tail
    for (; i < n; ++i) {
        if (!keys[i]) idx[lo++] = i;
        else          idx[--hi] = i;
    }
}


