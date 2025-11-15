#pragma once

template <unsigned int CORES = 4, bool Simd = true>
inline void radix_sort_uint64_mt(const uint64_t* keys,
                                size_t* idx,
                                size_t n)
{
    if (n == 0) return;

    constexpr unsigned THREADS = (CORES == 0 ? 1u : CORES);
    constexpr size_t PASSES = 4;

    // Storage
    std::vector<uint64_t> tkeys(n);
    std::vector<size_t>   tmp(n);

    // Pre-build transformed keys
    #pragma omp parallel for num_threads(THREADS)
    for (size_t i = 0; i < n; i++)
        tkeys[i] = keys[i];

    // Per-thread histograms
    std::vector<std::vector<size_t>>
        hist(THREADS, std::vector<size_t>(RADIX_KI64));

    std::vector<size_t> bucket_size(RADIX_KI64);
    std::vector<size_t> bucket_base(RADIX_KI64);

    std::vector<std::vector<size_t>>
        thread_off(THREADS, std::vector<size_t>(RADIX_KI64));

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
                    std::memset(h, 0, RADIX_KI64 * sizeof(size_t));
                    histogram_pass_u64_avx2(tkeys.data() + beg,
                                          len, shift, h);
                } else {
                    histogram_pass_u64_avx2_8buckets(tkeys.data() + beg,
                                                     len, shift, h);
                }
            } else
        #endif
            {
                std::memset(h, 0, RADIX_KI64 * sizeof(size_t));
                for (size_t i = beg; i < end; i++)
                    h[(tkeys[i] >> shift) & 0xFFFFu]++;
            }
        }

        // ----------------------------------------------------
        // Combine histograms
        // ----------------------------------------------------
        #pragma omp parallel for num_threads(THREADS)
        for (size_t b = 0; b < RADIX_KI64; b++) {
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
        for (size_t b = 0; b < RADIX_KI64; b++) {
            bucket_base[b] = acc;
            acc += bucket_size[b];
        }

        // ----------------------------------------------------
        // Thread offsets per bucket
        // ----------------------------------------------------
        #pragma omp parallel for num_threads(THREADS)
        for (size_t b = 0; b < RADIX_KI64; b++) {
            size_t base = bucket_base[b];
            for (unsigned t = 0; t < THREADS; t++) {
                thread_off[t][b] = base;
                base += hist[t][b];
            }
        }

        // ----------------------------------------------------
        // Parallel scatter
        // ----------------------------------------------------
        #pragma omp parallel num_threads(THREADS)
        #if defined(__AVX512F__) 
        if constexpr (Simd) {
            int t = omp_get_thread_num();
            auto [beg, end] = range(t);
            size_t len = end - beg;
            size_t* off = thread_off[t].data();
            
            scatter_pass_u64_avx512(
                tkeys + beg,   // local keys
                idx   + beg,   // local slice of current permutation
                len,
                shift,
                off,            // per-thread offsets
                tmp
            );
        } else
        #endif
        {
            int t = omp_get_thread_num();
            auto [beg, end] = range(t);
            size_t* off = thread_off[t].data();

            for (size_t i = beg; i < end; i++) {
                uint64_t b = (tkeys[i] >> shift) & 0xFFFFu;
                tmp[ off[b]++ ] = idx[i];
            }
        }

        std::memcpy(idx, tmp.data(), n * sizeof(size_t));

        // ----------------------------------------------------
        // Rebuild transformed keys
        // ----------------------------------------------------
        #pragma omp parallel for num_threads(THREADS)
        for (size_t i = 0; i < n; i++)
            tkeys[i] = keys[idx[i]];
    }
}



