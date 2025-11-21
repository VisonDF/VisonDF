#pragma once

template <unsigned int CORES = 4,
          bool Simd = true, 
          bool IsBoolCompressed = false>
inline void radix_sort_bool_u8_mt(uint8_t* keys, 
                            size_t* idx, 
                            size_t n) {

    #if !defined(__AVX2__)
    static_assert(!Simd, 
        "Simd=true requires AVX2, but AVX2 is not available on this CPU/compiler.");
    #endif

    if (n == 0) {
        warn("0 rows in radix_sort_bool_u8_mt");
        return;
    }

    if constexpr (Simd) {

        if constexpr (!IsBoolCompressed) {

            std::vector<size_t> count_false(CORES, 0);
            std::vector<size_t> count_true(CORES, 0);
            
            #pragma omp parallel n_threads(CORES)
            {

                int tid = omp_get_thread_num();
                size_t start = n * tid / CORES;
                size_t end   = n * (tid + 1) / CORES;
                
                size_t local_false = 0;
               
                #if defined (__AVX512F__)

                for (size_t i = start; i + 64 < end; i += 64) {
                    __m512i v = _mm512_loadu_si512((__m512i*)(keys + i));
                
                    __mmask64 mask = _mm512_cmpeq_epi8_mask(v, _mm512_setzero_si512());
                
                    local_false += _mm_popcnt_u64(mask);
                }
                
                for (size_t i = (end - (end - start) % 64); i < end; i++)
                    local_false += (keys[i] == 0);

                #elif defined (__AVX2__)

                for (size_t i = start; i + 32 < end; i += 32) {
                    __m256i v = _mm256_loadu_si256((__m256i*)(keys + i));
                
                    __m256i cmp = _mm256_cmpeq_epi8(v, _mm256_setzero_si256());
                
                    unsigned mask = _mm256_movemask_epi8(cmp);
                    local_false += __builtin_popcount(mask);
                }
                
                for (size_t i = (end - (end - start) % 32); i < end; i++)
                    local_false += (keys[i] == 0);
               
                #endif

                count_false[tid] = local_false;
                count_true[tid] = (end - start) - local_false;

            }
            
            std::vector<size_t> offset_false(CORES);
            std::vector<size_t> offset_true(CORES);
            
            offset_false[0] = 0;
            for (size_t t = 1; t < CORES; t++)
                offset_false[t] = offset_false[t-1] + count_false[t-1];
            
            offset_true[0] = n - count_true[0];
            for (size_t t = 1; t < CORES; t++)
                offset_true[t] = offset_true[t-1] - count_true[t];

            #if defined(__AVX512F__)
            #pragma omp parallel n_threads(CORES)
            {

                int tid = omp_get_thread_num();
                size_t start = n * tid / CORES;
                size_t end   = n * (tid+1) / CORES;
        
                size_t lo = offset_false[tid];
                size_t hi = offset_true[tid];

                avx512_bool_u8_mt(keys, end, idx, lo, hi, start);

            }
            #elif defined(__AVX2__)

            #pragma omp parallel n_threads(CORES)
            {

                int tid = omp_get_thread_num();
                size_t start = n * tid / CORES;
                size_t end   = n * (tid+1) / CORES;
        
                size_t lo = offset_false[tid];
                size_t hi = offset_true[tid];

                avx2_bool_u8_mt(keys, end, idx, lo, hi, start);

            }

            #endif


        } else if constexpr (IsBoolCompressed) {

                std::vector<size_t> count_false(CORES, 0);
                std::vector<size_t> count_true(CORES, 0);
                
                #pragma omp parallel n_threads(CORES)
                {

                    int tid = omp_get_thread_num();
                    size_t start = n * tid / CORES;
                    size_t end   = n * (tid + 1) / CORES;
                    
                    size_t local_false = 0;
 
                    #if defined (__AVX512BW__)
        
                    // LUT for nibble popcounts (0..15), repeated 4× to fill 64 bytes
                    static const __m512i POPCOUNT_TABLE_512 = _mm512_setr_epi8(
                        0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4,
                        0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4,
                        0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4,
                        0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4
                    );

                    size_t local_true = 0;

                    // byte range covering [start, end) bits
                    size_t i        = start >> 3;   // byte start
                    size_t end_byte = end   >> 3;   // byte end
                    size_t simd_limit = (end_byte >= 64) ? (end_byte - 64) : 0;

                    const __m512i low_mask = _mm512_set1_epi8(0x0F);
                    const __m512i zero     = _mm512_setzero_si512();

                    // Process 64 bytes per iteration
                    for (; i <= simd_limit; i += 64) {

                        __m512i bytes = _mm512_loadu_si512((const void*)(keys + i));

                        // Extract low and high nibbles
                        __m512i lo = _mm512_and_si512(bytes, low_mask);
                        __m512i hi = _mm512_and_si512(_mm512_srli_epi16(bytes, 4), low_mask);

                        // Lookup popcount per nibble
                        __m512i pc_lo = _mm512_shuffle_epi8(POPCOUNT_TABLE_512, lo);
                        __m512i pc_hi = _mm512_shuffle_epi8(POPCOUNT_TABLE_512, hi);

                        // Sum nibble popcounts → popcount per byte
                        __m512i pc = _mm512_add_epi8(pc_lo, pc_hi);

                        // Sum all 64 byte-popcounts into 8×64-bit lanes
                        __m512i sad = _mm512_sad_epu8(pc, zero);

                        alignas(64) uint64_t tmp[8];
                        _mm512_store_si512((__m512i*)tmp, sad);

                        uint64_t block_ones =
                              tmp[0] + tmp[1] + tmp[2] + tmp[3]
                            + tmp[4] + tmp[5] + tmp[6] + tmp[7];

                        local_true += block_ones;
                    }

                    // Scalar tail (bit-packed) - remaining bits
                    for (size_t bit = (i << 3); bit < end; ++bit) {
                        local_true += ( (keys[bit >> 3] >> (bit & 7)) & 1u );
                    }

                    #elif defined (__AVX2__)

                    // LUT for nibble popcounts (0..15)
                    static const __m256i POPCOUNT_TABLE = _mm256_setr_epi8(
                        0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4,
                        0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4
                    );
                    
                    size_t local_true = 0;
                    
                    size_t i = start >> 3;                  // byte start
                    size_t end_byte = end >> 3;             // byte end
                    size_t simd_limit = (end_byte >= 32) ? (end_byte - 32) : 0;
                    
                    const __m256i low_mask = _mm256_set1_epi8(0x0F);
                    const __m256i zero     = _mm256_setzero_si256();
                    
                    // Process 32 bytes per iteration
                    for (; i <= simd_limit; i += 32) {
                    
                        __m256i bytes = _mm256_loadu_si256((__m256i*)(keys + i));
                    
                        // Extract low and high nibbles 4/4
                        __m256i lo = _mm256_and_si256(bytes, low_mask);
                        __m256i hi = _mm256_and_si256(_mm256_srli_epi16(bytes, 4), low_mask);
                    
                        // Lookup popcount per nibble
                        __m256i pc_lo = _mm256_shuffle_epi8(POPCOUNT_TABLE, lo);
                        __m256i pc_hi = _mm256_shuffle_epi8(POPCOUNT_TABLE, hi);
                    
                        // Sum nibble popcounts = popcount per byte
                        __m256i pc = _mm256_add_epi8(pc_lo, pc_hi);
                    
                        // Sum all 32 byte-popcounts into 4×64-bit integers
                        __m256i sad = _mm256_sad_epu8(pc, zero);
                    
                        uint64_t block_ones =
                              (uint64_t)_mm256_extract_epi64(sad, 0)
                            + (uint64_t)_mm256_extract_epi64(sad, 1)
                            + (uint64_t)_mm256_extract_epi64(sad, 2)
                            + (uint64_t)_mm256_extract_epi64(sad, 3);
                    
                        local_true += block_ones;
                    }
                    
                    // scalar tail (bit-packed) - remaining bits
                    for (size_t bit = (i << 3); bit < end; bit++) {
                        local_true += ( (keys[bit >> 3] >> (bit & 7)) & 1 );
                    }

                    #endif

                    count_true[tid] = local_true;
                    count_false[tid] = (end - start) - local_true;

                }
                
                std::vector<size_t> offset_false(CORES);
                std::vector<size_t> offset_true(CORES);
                
                offset_false[0] = 0;
                for (size_t t = 1; t < CORES; t++)
                    offset_false[t] = offset_false[t-1] + count_false[t-1];
                
                offset_true[0] = n - count_true[0];
                for (size_t t = 1; t < CORES; t++)
                    offset_true[t] = offset_true[t-1] - count_true[t];


                #if defined (__AVX512BW__)

                #pragma omp parallel n_threads(CORES)
                {

                    int tid = omp_get_thread_num();
                    size_t start = n * tid / CORES;
                    size_t end   = n * (tid + 1) / CORES;
        
                    size_t lo = offset_false[tid];
                    size_t hi = offset_true[tid];

                    avx512_bool_compressed_mt(keys, end, idx, lo, hi, start);

                }

                #elif defined(__AVX2___)

                #pragma omp parallel n_threads(CORES)
                {

                    int tid = omp_get_thread_num();
                    size_t start = n * tid / CORES;
                    size_t end   = n * (tid + 1) / CORES;
        
                    size_t lo = offset_false[tid];
                    size_t hi = offset_true[tid];

                    avx2_bool_compressed_mt(keys, end, idx, lo, hi, start);

                }

                #endif

        }

    } else if constexpr (!Simd) {

        if constexpr (IsBoolCompressed) {
          std::cerr << "Error, IsBoolCompressed must be aplied with SIMD";
          return;
        }

        std::vector<size_t> count_false(CORES, 0);
        std::vector<size_t> count_true(CORES, 0);
        
        #pragma omp parallel n_threads(CORES)
        {
            int tid = omp_get_thread_num();
            size_t start = n * tid / CORES;
            size_t end   = n * (tid + 1) / CORES;
        
            size_t local_false = 0;
            size_t local_true = 0;
        
            for (size_t i = start; i < end; i++) {
                if (!keys[i]) local_false++;
                else local_true++;
            }
        
            count_false[tid] = local_false;
            count_true[tid]  = (end - start) - local_false;
        }
        
        std::vector<size_t> offset_false(CORES);
        std::vector<size_t> offset_true(CORES);
        
        offset_false[0] = 0;
        for (size_t t = 1; t < CORES; t++)
            offset_false[t] = offset_false[t-1] + count_false[t-1];
        
        offset_true[0] = n - count_true[0];
        for (size_t t = 1; t < CORES; t++)
            offset_true[t] = offset_true[t-1] - count_true[t];
        
        #pragma omp parallel n_threads(CORES)
        {
            int tid = omp_get_thread_num();
            size_t start = n * tid / CORES;
            size_t end   = n * (tid+1) / CORES;
        
            size_t lo = offset_false[tid];
            size_t hi = offset_true[tid];
        
            for (size_t i = start; i < end; i++) {
                if (!keys[i]) idx[lo++] = i;
                else          idx[--hi] = i;
            }
        }

    }
    
}


