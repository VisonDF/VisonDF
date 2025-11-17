#pragma once

#if defined(__AVX512F__)

// Only to be used when bool is used with Compression Mode

//64 bytes
// ↓
//Extract bit 0 across all bytes → 64 booleans → scatter
//Extract bit 1 across all bytes → 64 booleans → scatter
//Extract bit 2 across all bytes → 64 booleans → scatter
//...
//Extract bit 7 across all bytes → 64 booleans → scatter

// keys: bit-packed booleans, k-th boolean is bit (k & 7) of keys[k >> 3]
// end: number of boolean values
// idx: output indices, partitioned: all false first, then all true
inline void avx512_bool_compressed_mt(const uint8_t *keys,
                                      size_t end,
                                      size_t *idx,
                                      size_t lo,
                                      size_t hi,
                                      size_t start)
{

    // Process full blocks of 512 bits = 64 bytes
    while (start + 512 <= end) {
        size_t byte_off = start >> 3; // = (start / 8)
        __m512i v = _mm512_loadu_si512((const __m512i *)(keys + byte_off));

        static const __m512i MASKS[8] = {
            _mm512_set1_epi8(1 << 0),
            _mm512_set1_epi8(1 << 1),
            _mm512_set1_epi8(1 << 2),
            _mm512_set1_epi8(1 << 3),
            _mm512_set1_epi8(1 << 4),
            _mm512_set1_epi8(1 << 5),
            _mm512_set1_epi8(1 << 6),
            _mm512_set1_epi8(1 << 7)
        };

        int bit_offset  = start & 7; // may start at a non brand new byte

        // For each bit-plane inside these 64 bytes (8 bits per byte)
        for (int b = bit_offset; b < 8; ++b) {
            // Isolate bit b in each byte: mask = (1 << b)
            __m512i mask_b = MASKS[b];
            __m512i masked  = _mm512_and_si512(v, mask_b);

            // Shift that bit up to the sign bit (bit 7) so movemask can see it
            __m512i shifted = _mm512_slli_epi16(masked, 7 - b);

            // movemask: 1 bit per byte, 64 bits for 64 bytes
            // will lace the corresponding boolean bit at the first position of all 64 bytes
            uint64_t m = (uint64_t)_mm512_movemask_epi8(shifted);

            uint64_t bits = m;
            
            // NOTE: we compute (start + b) once:
            size_t base_b = start + (size_t)b;
            
            #define HANDLE(I) do { \
                size_t logical = base_b + ((size_t)(I) << 3); \
                if ((bits & (1ull << (I))) == 0) idx[lo++] = logical; \
                else                           idx[--hi] = logical; \
            } while (0)
            
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

        }

        start += 512;
    }

    // Tail: handle remaining [start, end) scalar
    for (; start < end; ++start) {
        size_t byte_index = start >> 3;
        int    bit_index  = (int)(start & 7u);

        uint8_t byte = keys[byte_index];
        uint8_t val  = (byte >> bit_index) & 1u; // 1 = true, 0 = false

        if (val == 0) {
            idx[lo++] = start;
        } else {
            idx[--hi] = start;
        }
    }
}

#endif


