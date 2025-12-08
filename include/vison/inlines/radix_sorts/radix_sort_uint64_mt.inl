#pragma once

template <unsigned int CORES = 4, bool Simd = true>
inline void radix_sort_uint64_mt(std::vector<uint64_t>& tkeys,
                                size_t* idx,
                                size_t n)
{

    #if !defined(__AVX2__)
    static_assert(!Simd, 
        "Simd=true requires AVX2, but AVX2 is not available on this CPU/compiler.");
    #endif

    if (n == 0) {
        warn("0 rows in radix_sort_uint64_mt");
        return;
    }

    constexpr unsigned THREADS = (CORES == 0 ? 1u : CORES);
    constexpr size_t PASSES = 4;

    // Storage
    std::vector<uint64_t> tmp_keys(n);
    std::vector<size_t>   tmp(n);

    // Per-thread histograms
    std::vector<std::vector<size_t>>& hist = get_local_hist_u16();
    std::vector<std::vector<size_t>>& thread_off = get_local_thread_off_u16();
    #pragma omp parallel for if(THREADS > 1) num_threads(THREADS)
    for (size_t i = 0; i < THREADS; ++i) {
        memset(hist[i].data(),       0, RADIX_KI16 * sizeof(size_t));
        memset(thread_off[i].data(), 0, RADIX_KI16 * sizeof(size_t));
    }

    std::vector<size_t>& bucket_size = get_local_bucket_size_u16();
    std::vector<size_t>& bucket_base = get_local_bucket_base_u16();
    memset(bucket_size.data(), 0, RADIX_KI16 * sizeof(size_t));
    memset(bucket_base.data(), 0, RADIX_KI16 * sizeof(size_t));

    auto range = [&](int t) {
        size_t chunk = n / THREADS;
        size_t rem   = n % THREADS;
        size_t begin = t * chunk + std::min<size_t>(t, rem);
        size_t end   = begin + chunk + (t < rem ? 1 : 0);
        return std::pair<size_t,size_t>(begin, end);
    };

    for (size_t pass = 0; pass < PASSES; pass++)
    {
        const size_t shift = pass * 16;

        // ----------------------------------------------------
        // Parallel histogram
        // ----------------------------------------------------
        #pragma omp parallel num_threads(THREADS)
        {
            int t = omp_get_thread_num();
            auto [beg, end] = range(t);

            size_t* h = hist[t].data();
            size_t len = end - beg;

        #if defined(__AVX512F__)
            if constexpr (Simd) {
                histogram_pass_u64_avx512_16buckets(tkeys.data() + beg,
                                                    len, shift, h);
            } else
        #endif
        #if defined(__AVX2__)
            if constexpr (Simd) {
                if (len < 200000) {
                    std::memset(h, 0, RADIX_KI16 * sizeof(size_t));
                    histogram_pass_u64_avx2(tkeys.data() + beg,
                                            len, shift, h);
                } else {
                    histogram_pass_u64_avx2_8buckets(tkeys.data() + beg,
                                                     len, shift, h);
                }
            } else
        #endif
            {
                std::memset(h, 0, RADIX_KI16 * sizeof(size_t));
                for (size_t i = beg; i < end; i++)
                    h[(tkeys[i] >> shift) & 0xFFFFu]++;
            }
        }

        // ----------------------------------------------------
        // Combine histograms
        // ----------------------------------------------------
        #pragma omp parallel for num_threads(THREADS)
        for (size_t b = 0; b < RADIX_KI16; b++) {
            size_t sum = 0;

            #pragma unroll
            for (unsigned t = 0; t < THREADS; t++)
                sum += hist[t][b];
            bucket_size[b] = sum;
        }

        // ----------------------------------------------------
        // Prefix sum
        // ----------------------------------------------------
        size_t acc = 0;
        for (size_t b = 0; b < RADIX_KI16; b++) {
            bucket_base[b] = acc;
            acc += bucket_size[b];
        }

        // ----------------------------------------------------
        // Thread offsets per bucket
        // ----------------------------------------------------
        #pragma omp parallel for num_threads(THREADS)
        for (size_t b = 0; b < RADIX_KI16; b++) {
            size_t base = bucket_base[b];
            for (unsigned t = 0; t < THREADS; t++) {
                thread_off[t][b] = base;
                base += hist[t][b];
            }
        }

        // ----------------------------------------------------
        // Parallel scatter
        // ----------------------------------------------------
        // safe to parallelize because no dupplicates in counts / offs
        #pragma omp parallel num_threads(THREADS)
        #if defined(__AVX512F__) 
        if constexpr (Simd) {
            int t = omp_get_thread_num();
            auto [beg, end] = range(t);
            size_t len = end - beg;
            size_t* off = thread_off[t].data();
            
            scatter_pass_u64_avx512(
                tkeys.data() + beg, // per-thread input keys
                idx     + beg,      // per-thread input indices
                len,                // size of chunk
                shift,
                off, // per-thread offsets
                tmp.data(),           // global output idx
                tmp_keys.data()       // global output keys
            );

        } else
        #endif
        {
            int t = omp_get_thread_num();
            auto [beg, end] = range(t);
            size_t* off = thread_off[t].data();

            for (size_t i = beg; i < end; i++) {
                uint64_t key = tkeys[i];
                uint64_t b   = (key >> shift) & 0xFFFF;
                size_t pos = off[b]++;
                tmp[pos]      = idx[i];
                tmp_keys[pos] = key;
            }
        }

        std::swap(tkeys, tmp_keys);
        std::memcpy(idx, tmp.data(), n * sizeof(size_t));

    }
}



