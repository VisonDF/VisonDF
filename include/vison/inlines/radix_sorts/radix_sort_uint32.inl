#pragma once

template <bool Simd = true>
inline void radix_sort_uint32(const uint32_t* keys,
                             size_t* idx,
                             size_t n)
{

    #if !defined(__AVX2__)
    static_assert(!Simd, 
        "Simd=true requires AVX2, but AVX2 is not available on this CPU/compiler.");
    #endif

    if (n == 0) {
        warn("0 rows in radix_sort_uint32");
        return;
    }

    using U = uint32_t;
    constexpr size_t PASSES = 2;      

    std::vector<U> tkeys(n);
    std::vector<U> tmp_keys(n);
    std::vector<size_t> tmp(n);
    std::vector<size_t> count(RADIX_KI32);

    for (size_t i = 0; i < n; i++)
        tkeys[i] = keys[i];

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
                memset(count.data(), 0, RADIX_KI32 * sizeof(size_t));
                histogram_pass_u32_avx2(tkeys.data(), n, shift, count.data());
            } else {
                histogram_pass_u32_avx2_8buckets(tkeys.data(), n, shift, count.data());
            }
        #endif
        
        } else {
            memset(count.data(), 0, RADIX_KI32 * sizeof(size_t));
            for (size_t i = 0; i < n; ++i)
                count[(tkeys[i] >> shift) & 0xFFFF]++;
        }
                                                   
        // Convert histogram to prefix sums:
        // count[b] becomes the starting output index for bucket b.
        size_t sum = 0;
        for (size_t i = 0; i < RADIX_KI32; i++) {
            size_t c = count[i];
            count[i] = sum;
            sum += c;
        }

        // Stable scatter:
        // Place each index into its correct bucket position in tmp[].
        #if defined(__AVX512F__)
            if constexpr (Simd) {
                scatter_pass_u32_avx512(
                    tkeys.data(),
                    idx,
                    n,
                    shift,
                    count.data(),
                    tmp.data(),
                    tmp_keys.data()
                );
            } else
        #endif
            {
                for (size_t i = 0; i < n; i++) {
                    U key = tkeys[i];
                    U b   = (key >> shift) & 0xFFFF;
                    size_t   pos = count[b]++;

                    tmp[pos]  = idx[i];
                    tmp_keys[pos] = key;
                }
            }

        std::swap(tmp_keys, tkeys);
        memcpy(idx, tmp.data(), n * sizeof(size_t));

    }
}

