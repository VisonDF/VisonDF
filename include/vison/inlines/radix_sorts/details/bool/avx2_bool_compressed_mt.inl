#pragma once

// Only to be used when bool is used with Compression Mode

//32 bytes
// ↓
//Extract bit 0 across all bytes → 32 booleans → scatter
//Extract bit 1 across all bytes → 32 booleans → scatter
//Extract bit 2 across all bytes → 32 booleans → scatter
//...
//Extract bit 7 across all bytes → 32 booleans → scatter

// keys: bit-packed booleans, k-th boolean is bit (k & 7) of keys[k >> 3]
// end: number of boolean values
// idx: output indices, partitioned: all false first, then all true
inline void avx2_bool_compressed_mt(const uint8_t *keys,
                                    size_t end,
                                    size_t *idx,
                                    size_t lo,
                                    size_t hi,
                                    size_t start)
{

    // Process full blocks of 256 bits = 32 bytes
    while (start + 256 <= end) {
        size_t byte_off = start >> 3; // = (start / 8)
        __m256i v = _mm256_loadu_si256((const __m256i *)(keys + byte_off));

        static const __m256i MASKS[8] = {
            _mm256_set1_epi8(1 << 0),
            _mm256_set1_epi8(1 << 1),
            _mm256_set1_epi8(1 << 2),
            _mm256_set1_epi8(1 << 3),
            _mm256_set1_epi8(1 << 4),
            _mm256_set1_epi8(1 << 5),
            _mm256_set1_epi8(1 << 6),
            _mm256_set1_epi8(1 << 7)
        };

        int bit_offset  = start & 7; // may start at a non brand new byte

        // For each bit-plane inside these 32 bytes (8 bits per byte)
        for (int b = bit_offset; b < 8; ++b) {
            // Isolate bit b in each byte: mask = (1 << b)
            __m256i mask_b = MASKS[b];
            __m256i masked  = _mm256_and_si256(v, mask_b);

            // Shift that bit up to the sign bit (bit 7) so movemask can see it
            __m256i shifted = _mm256_slli_epi16(masked, 7 - b);

            // movemask: 1 bit per byte, 32 bits for 32 bytes
            // will lace the corresponding boolean bit at the first position of all 32 bytes
            uint32_t m = (uint32_t)_mm256_movemask_epi8(shifted);

            uint32_t bits = m;
            
            // NOTE: we compute (start + b) once:
            size_t base_b = start + (size_t)b;
            
            #define HANDLE(I) do { \
                size_t logical = base_b + ((size_t)(I) << 3); \
                if ((bits & (1u << (I))) == 0) idx[lo++] = logical; \
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
            
            #undef HANDLE

        }

        start += 256;
    }

    // Tail: handle remaining [start, end) scalar
    for (; start < end; ++start) {
        size_t byte_index = start >> 3;  // current byte (divide by 2^3)

        int    bit_index  = (int)(start & 7u); 
        // Index of the bit inside the current byte (0..7).
        // start & 7 extracts the lowest 3 bits of start (equivalent to start % 8).
        // As start increments, this cycles 0→7, visiting all bits of the byte.

        uint8_t byte = keys[byte_index];
        uint8_t val  = (byte >> bit_index) & 1u; // 1 = true, 0 = false

        if (val == 0) {
            idx[lo++] = start;
        } else {
            idx[--hi] = start;
        }
    }
}


