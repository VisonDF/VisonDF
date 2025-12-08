#pragma once

template <bool Simd = true,
          bool MultiLanes = true>
inline void radix_sort_uint64(std::vector<uint64_t>& tkeys, 
                              size_t* idx, 
                              size_t n)
{

    #if !defined(__AVX2__)
    static_assert(!Simd, 
        "Simd=true requires AVX2, but AVX2 is not available on this CPU/compiler.");
    #endif

    if (n == 0) {
        warn("0 rows in radix_sort_uint64");
        return;
    }

    std::vector<size_t>& count = get_local_count_u16();
    memset(count.data(), 0, RADIX_KI16 * sizeof(size_t));
    std::vector<size_t> tmp_idx(n);
    std::vector<uint64_t> tmp_keys(n);

    for (size_t pass = 0; pass < 4; pass++)
    {
        size_t shift = pass * 16;

        if constexpr (Simd) {
        
        #if defined(__AVX512F__)
            histogram_pass_u64_avx512_16buckets(tkeys.data(), n, shift, count.data());
            
        #elif defined(__AVX2__)
            if constexpr (!MultiLanes) {
                memset(count.data(), 0, RADIX_KI16 * sizeof(size_t));
                histogram_pass_u64_avx2(tkeys.data(), n, shift, count.data());
            } else {
                histogram_pass_u64_avx2_8buckets(tkeys.data(), n, shift, count.data());
            }
        #endif
        
        } else {
            memset(count.data(), 0, RADIX_KI16 * sizeof(size_t));
            for (size_t i = 0; i < n; ++i)
                count[(tkeys[i] >> shift) & 0xFFFF]++;
        }

        size_t sum = 0;
        for (size_t i = 0; i < RADIX_KI16; i++) {
            size_t c = count[i];
            count[i] = sum;
            sum += c;
        }

        #if defined(__AVX512F__)
            if constexpr (Simd) {
                scatter_pass_u64_avx512(
                    tkeys.data(),
                    idx,
                    n,
                    shift,
                    count.data(),
                    tmp_idx.data(),
                    tmp_keys.data()
                );
            } else
        #endif
            {
                for (size_t i = 0; i < n; i++) {
                    uint64_t key = tkeys[i];
                    uint64_t b   = (key >> shift) & 0xFFFF;
                    size_t   pos = count[b]++;

                    tmp_idx[pos]  = idx[i];
                    tmp_keys[pos] = key;
                }
            }

        std::swap(tmp_keys, tkeys);
        memcpy(idx, tmp_idx.data(), n * sizeof(size_t));

    }
}


