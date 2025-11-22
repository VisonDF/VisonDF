#pragma once

template<unsigned int CORES = 4, bool Simd = true>
inline void radix_sort_charbuf_flat_mt(
    const uint8_t* bufv,  
    const size_t n,
    const size_t df_charbuf_size,
    size_t* idx
)
{
    #if !defined(__AVX2__) && !defined(__AVX512F__)
    static_assert(!Simd,
        "Simd=true requires AVX2 or AVX512F, but neither is available.");
    #endif

    if (n == 0) {
        warn("0 rows in radix_sort_charbuf_flat_mt");
        return;
    }

    #if defined(__AVX512F__)
    constexpr bool HasAVX512 = true;
    #else
    constexpr bool HasAVX512 = false;
    #endif
    constexpr bool InplaceScatter = Simd && HasAVX512;

    constexpr unsigned THREADS = (CORES == 0 ? 1u : CORES);

    std::vector<size_t>  tmp(n);
    std::vector<uint8_t> cur_keys(n);

    size_t* in  = idx;
    size_t* out = tmp.data();

    // Shared MT structures (reused for each digit)
    std::vector<std::vector<size_t>> hist(THREADS, std::vector<size_t>(256));
    std::vector<size_t> bucket_size(256);
    std::vector<size_t> bucket_base(256);
    std::vector<std::vector<size_t>> thread_off(THREADS, std::vector<size_t>(256));

    auto range = [&](int t) {
        size_t chunk = n / THREADS;
        size_t rem   = n % THREADS;
        size_t beg   = t * chunk + std::min<size_t>(t, rem);
        size_t end   = beg + chunk + (t < rem ? 1 : 0);
        return std::pair<size_t,size_t>(beg, end);
    };

    for (int pos = int(df_charbuf_size) - 1; pos >= 0; --pos) {

        // 0) Build current digit keys according to 'in'
        for (size_t i = 0; i < n; ++i) {
            cur_keys[i] = bufv[in[i] * df_charbuf_size + pos];
        }

        // 1) PER-THREAD HISTOGRAMS ON cur_keys
        #pragma omp parallel num_threads(THREADS)
        {
            int t = omp_get_thread_num();
            auto [beg, end] = range(t);

            size_t* h = hist[t].data();
            std::memset(h, 0, 256 * sizeof(size_t));

            if constexpr (Simd) {
                size_t len = end - beg;

                #if defined(__AVX512F__)
                    histogram_pass_u8_avx512_16buckets(
                        cur_keys.data() + beg,
                        len,
                        h
                    );
                #elif defined(__AVX2__)
                    if (len < 200'000) {
                        histogram_pass_u8_avx2(
                            cur_keys.data() + beg,
                            len,
                            h
                        );
                    } else {
                        histogram_pass_u8_avx2_8buckets(
                            cur_keys.data() + beg,
                            len,
                            h
                        );
                    }
                #endif
            } else {
                for (size_t i = beg; i < end; ++i)
                    h[cur_keys[i]]++;
            }
        }

        // 2) COMBINE HISTOGRAMS → bucket_size
        #pragma omp parallel for num_threads(THREADS)
        for (int b = 0; b < 256; b++) {
            size_t sum = 0;
            #pragma unroll
            for (unsigned t = 0; t < THREADS; t++)
                sum += hist[t][b];
            bucket_size[b] = sum;
        }

        // 3) GLOBAL PREFIX → bucket_base
        size_t acc = 0;
        for (int b = 0; b < 256; b++) {
            bucket_base[b] = acc;
            acc += bucket_size[b];
        }

        // 4) PER-THREAD BUCKET OFFSETS
        #pragma omp parallel for num_threads(THREADS)
        for (int b = 0; b < 256; b++) {
            size_t base = bucket_base[b];
            for (unsigned t = 0; t < THREADS; t++) {
                thread_off[t][b] = base;
                base += hist[t][b];
            }
        }

        // 5) PARALLEL STABLE SCATTER
        #pragma omp parallel num_threads(THREADS)
        {
            int t = omp_get_thread_num();
            auto [beg, end] = range(t);
            size_t* off = thread_off[t].data();
            size_t len = end - beg;

        #if defined(__AVX512F__)
            if constexpr (InplaceScatter) {
                // AVX512 path: scatter modifies 'in' in-place
                scatter_pass_u8_avx512_mt(
                    cur_keys.data() + beg, // keys for [beg, end)
                    in,                    // index array, modified in-place
                    beg,
                    len,
                    off
                );
            } else
        #endif
            {
                // Scalar stable scatter (or AVX2 + scalar scatter)
                for (size_t i = beg; i < end; i++) {
                    uint8_t k = cur_keys[i];
                    out[off[k]++] = in[i];
                }
            }
        }

        if constexpr (!InplaceScatter)
            std::swap(in, out);
    }

    if constexpr (!InplaceScatter) {
        if (in != idx) {
            std::copy(in, in + n, idx);
        }
    }
}


