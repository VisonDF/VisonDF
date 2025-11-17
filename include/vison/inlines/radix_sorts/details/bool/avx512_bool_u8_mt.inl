#pragma once

#if defined(__AVX512F__)

// keys: byte-per-bool (0 = false, non-zero = true)
// idx: output indices, partitioned: all false first, then all true
static inline void avx512_bool_u8_mt(const uint8_t* __restrict keys,
                                     size_t end,
                                     size_t* __restrict idx,
                                     size_t lo,
                                     size_t hi,
                                     size_t start)
{

    const __m512i zero = _mm512_setzero_si512();

    // Process full blocks of 64 bytes at a time
    while (start + 64 <= end) {
        // Load 64 bytes = 64 booleans
        __m512i v = _mm512_loadu_si512((const void*)(keys + start));

        // Compare each byte with zero:
        // result bit j == 1  -> keys[start + j] == 0 (false)
        // result bit j == 0  -> keys[start + j] != 0 (true)
        __mmask64 false_mask = _mm512_cmpeq_epi8_mask(v, zero);

        const uint64_t fm = (uint64_t)false_mask;

        // Scatter 64 indices according to the mask
        #define HANDLE(J) do { \
            size_t logical = start + (J); \
            if (fm & (1ull << (J))) \
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
        HANDLE(32); HANDLE(33); HANDLE(34); HANDLE(35);
        HANDLE(36); HANDLE(37); HANDLE(38); HANDLE(39);
        HANDLE(40); HANDLE(41); HANDLE(42); HANDLE(43);
        HANDLE(44); HANDLE(45); HANDLE(46); HANDLE(47);
        HANDLE(48); HANDLE(49); HANDLE(50); HANDLE(51);
        HANDLE(52); HANDLE(53); HANDLE(54); HANDLE(55);
        HANDLE(56); HANDLE(57); HANDLE(58); HANDLE(59);
        HANDLE(60); HANDLE(61); HANDLE(62); HANDLE(63);

        #undef HANDLE

        start += 64;
    }

    // Scalar tail
    for (; start < end; ++start) {
        if (!keys[start]) {
            idx[lo++] = start;
        } else {
            idx[--hi] = start;
        }
    }
}

#endif


