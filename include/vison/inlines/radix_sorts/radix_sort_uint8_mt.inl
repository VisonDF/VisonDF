#pragma once

template <unsigned int CORES = 4, bool Simd = true>
inline void radix_sort_uint8_mt(const uint8_t* keys,
                                size_t* idx,
                                size_t n)
{
    if (n == 0) return;

    constexpr unsigned THREADS = (CORES == 0 ? 1u : CORES);

    // Working storage
    std::vector<std::vector<size_t>>
        hist(THREADS, std::vector<size_t>(RADIX_KI8));

    std::vector<size_t> bucket_size(RADIX_KI8);
    std::vector<size_t> bucket_base(RADIX_KI8);

    std::vector<std::vector<size_t>>
        thread_off(THREADS, std::vector<size_t>(RADIX_KI8));

    auto range = [&](int t) {
        size_t chunk = n / THREADS;
        size_t rem   = n % THREADS;
        size_t beg   = t * chunk + std::min<size_t>(t, rem);
        size_t end   = beg + chunk + (t < rem ? 1 : 0);
        return std::pair<size_t,size_t>(beg, end);
    };

    // ====================================================
    // 1) PER-THREAD HISTOGRAMS
    // ====================================================
    #pragma omp parallel num_threads(THREADS)
    {
        int t = omp_get_thread_num();
        auto [beg, end] = range(t);

        size_t* h = hist[t].data();

#if defined(__AVX512F__) 
        if constexpr (Simd) {
            histogram_pass_u8_avx512_16buckets(keys + beg,
                                               end - beg,
                                               h);
        } else
#endif
#if defined(__AVX2__)
        if constexpr (Simd) {
            if (end - beg < 200'000)
                histogram_pass_u8_avx2(keys + beg, end - beg, h);
            else
                histogram_pass_u8_avx2_8buckets(keys + beg, end - beg, h);
        } else
#endif
        {
            for (size_t i = beg; i < end; i++)
                h[keys[i]]++;
        }
    }

    // ====================================================
    // 2) COMBINE HISTOGRAMS
    // ====================================================
    #pragma omp parallel for num_threads(THREADS)
    for (size_t b = 0; b < RADIX_KI8; b++) {
        size_t sum = 0;

        #pragma unroll
        for (unsigned t = 0; t < THREADS; t++)
            sum += hist[t][b];
        bucket_size[b] = sum;
    }

    // ====================================================
    // 3) GLOBAL PREFIX
    // ====================================================
    size_t acc = 0;
    for (size_t b = 0; b < RADIX_KI8; b++) {
        bucket_base[b] = acc;
        acc += bucket_size[b];
    }

    // ====================================================
    // 4) PER-THREAD BUCKET OFFSETS
    // ====================================================
    #pragma omp parallel for num_threads(THREADS)
    for (size_t b = 0; b < RADIX_KI8; b++) {
        size_t base = bucket_base[b];
        for (unsigned t = 0; t < THREADS; t++) {
            thread_off[t][b] = base;
            base += hist[t][b];
        }
    }

    // ====================================================
    // 5) PARALLEL STABLE SCATTER
    // ====================================================
    #pragma omp parallel num_threads(THREADS)
    #if defined(__AVX512F__) 
    if constexpr (Simd) {
        int t = omp_get_thread_num();
        auto [beg, end] = range(t);
        size_t len = end - beg;
        size_t* off = thread_off[t].data();
        
        scatter_pass_u8_avx512_mt(
            keys + beg,   
            idx,          
            beg,          
            len,
            off           
        );
    } else
    #endif
        {
            int t = omp_get_thread_num();
            auto [beg, end] = range(t);

            size_t* off = thread_off[t].data();

            for (size_t i = beg; i < end; i++) {
                uint8_t k = keys[i];
                idx[ off[k]++ ] = i;
            }
        }

}


