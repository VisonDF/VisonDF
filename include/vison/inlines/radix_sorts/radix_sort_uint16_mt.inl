#pragma once

template <unsigned int CORES = 4, bool Simd = true>
inline void radix_sort_uint16_mt(const uint16_t* keys,
                                size_t* idx,
                                size_t n)
{

    #if !defined(__AVX2__)
    static_assert(!Simd, 
        "Simd=true requires AVX2, but AVX2 is not available on this CPU/compiler.");
    #endif

    if (n == 0) {
        warn("0 rows in radix_sort_uint16_mt");
        return;
    }

    constexpr unsigned THREADS = (CORES == 0 ? 1u : CORES);

    // Working storage
    std::vector<std::vector<size_t>>& hist = get_local_hist_u16();
    std::vector<std::vector<size_t>>& thread_off = get_local_thread_off_u16();
    #pragma omp parallel for if(THREADS > 1) num_threads(THREADS)
    for (size_t i = 0; i < THREADS: ++i) { 
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
            histogram_pass_u16_avx512_16buckets(keys + beg,
                                               end - beg,
                                               h);
        } else
        #endif
        #if defined(__AVX2__)
        if constexpr (Simd) {
            if (end - beg < 200'000)
                histogram_pass_u16_avx2(keys + beg, end - beg, h);
            else
                histogram_pass_u16_avx2_8buckets(keys + beg, end - beg, h);
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
    for (size_t b = 0; b < RADIX_KI16; b++) {
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
    for (size_t b = 0; b < RADIX_KI16; b++) {
        bucket_base[b] = acc;
        acc += bucket_size[b];
    }

    // ====================================================
    // 4) PER-THREAD BUCKET OFFSETS
    // ====================================================
    #pragma omp parallel for num_threads(THREADS)
    for (size_t b = 0; b < RADIX_KI16; b++) {
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
        
        scatter_pass_u16_avx512_mt(
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
            uint16_t k = keys[i];
            idx[ off[k]++ ] = i;
        }
    }

}


