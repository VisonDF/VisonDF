#pragma once

template <bool ASC, 
          unsigned int CORES = 4, 
          bool Simd = true,
          bool Flat = false,
          SortType S = SortType::Radix,
          typename ComparatorFactory = DefaultComparatorFactory>
inline void sort_char(
    std::vector<size_t>& idx,
    const int8_t (*col)[df_charbuf_size],
    const size_t nrow,
    const size_t df_charbuf_size,
    ComparatorFactory make_cmp = ComparatorFactory{}
)
{

    auto cmp = make_cmp.template operator()<ASC, CharT>(col);
    static_assert(IndexComparator<decltype(cmp)>,
              "Comparator must be cmp(size_t,size_t)->bool");

    if constexpr (S == SortType::Radix) {

            if constexpr (Flat) {
                
                std::vector<uint8_t> keys_flat(nrow * df_charbuf_size);

                if constexpr (CORES > 1) {

                    if constexpr (Simd) {

                        #pragma omp parallel for num_threads(CORES)
                        for (size_t i = 0; i < CORES; ++i) {

                            size_t t = omp_get_thread_num();
                            const size_t chunk = nrow / CORES;
                            const size_t rem   = nrow % CORES;
                            const size_t start = t * chunk + std::min(t, rem);
                            const size_t end   = start + chunk + (t < rem ? 1 : 0);

                            #if defined (__AVX512F__)
                            char_to_u8buf_avx512(keys_flat.data(), 
                                                 col, 
                                                 start,
                                                 end,
                                                 df_charbuf_size); 
                            #else
                            char_to_u8buf_avx2(keys_flat.data(), 
                                               col, 
                                               start,
                                               end,
                                               df_charbuf_size); 
                            #endif
                   
                        }

                    } else if constexpr (!Simd) {

                            #pragma omp parallel num_threads(CORES)
                            {

                                size_t t = omp_get_thread_num();
                                const size_t chunk = nrow / CORES;
                                const size_t rem   = nrow % CORES;
                                const size_t start = t * chunk + std::min(t, rem);
                                const size_t end   = start + chunk + (t < rem ? 1 : 0);

                                for (size_t i = start; i < end; ++i) {
                                    uint8_t* dst = keys_flat.data() + i * df_charbuf_size;
                                    const int8_t* src = col[i];
                                
                                    for (size_t j = 0; j < df_charbuf_size; ++j)
                                        dst[j] = uint8_t(src[j]) ^ 0x80u;
                                }

                            }

                    }

                    radix_sort_charbuf_flat_mt<CORES, Simd>(keys_flat.data(), 
                                                            nrow,
                                                            df_charbuf_size,
                                                            idx.data());              
                } else if constexpr (CORES <= 1) {

                    if constexpr (Simd) {

                        #if defined (__AVX512F__)
                        char_to_u8buf_avx512(keys_flat.data(), 
                                             col, 
                                             0,
                                             nrow,
                                             df_charbuf_size); 
                        #else
                        char_to_u8buf_avx2(keys_flat.data(), 
                                           col, 
                                           0,
                                           nrow,
                                           df_charbuf_size); 
                        #endif

                    } else if constexpr (!Simd) {

                        for (size_t i = 0; i < nrow; ++i) {
                            uint8_t* dst = keys_flat.data() + i * df_charbuf_size;
                            const int8_t* src = col[i];
                        
                            for (size_t j = 0; j < df_charbuf_size; ++j)
                                dst[j] = uint8_t(src[j]) ^ 0x80u;
                        }

                    }

                    radix_sort_charbuf_flat<Simd>(keys_flat.data(), 
                                                  nrow,
                                                  df_charbuf_size,
                                                  idx.data());

                }

            } else if constexpr (!Flat) {

                std::vector<uint8_t[df_charbuf_size]> tkeys(nrow);

                if constexpr (CORES > 1) {

                    if constexpr (Simd) {

                        #pragma omp parallel for num_threads(CORES)
                        for (size_t i = 0; i < CORES; ++i) {

                            size_t t = omp_get_thread_num();
                            const size_t chunk = nrow / CORES;
                            const size_t rem   = nrow % CORES;
                            const size_t start = t * chunk + std::min(t, rem);
                            const size_t end   = start + chunk + (t < rem ? 1 : 0);

                            #if defined (__AVX512F__)
                            char_to_u8buf2d_avx512<df_charbuf_size>(tkeys.data(), 
                                                                    col, 
                                                                    start,
                                                                    end,
                                                                    df_charbuf_size); 
                            #else
                            char_to_u8buf2d_avx2<df_charbuf_size>(tkeys.data(), 
                                                                  col, 
                                                                  start,
                                                                  end,
                                                                  df_charbuf_size); 
                            #endif

                        }

                    } else if constexpr (!Simd) {

                            #pragma omp parallel num_threads(CORES)
                            {

                                size_t t = omp_get_thread_num();
                                const size_t chunk = nrow / CORES;
                                const size_t rem   = nrow % CORES;
                                const size_t start = t * chunk + std::min(t, rem);
                                const size_t end   = start + chunk + (t < rem ? 1 : 0);

                                for (size_t i = 0; i < nrow; ++i) {
                                    const int8_t (&cur_col)[df_charbuf_size] = col[i];
                                    std::vector<uint8_t[df_charbuf_size]>& tmp_tkeys = tkeys[i];
                                    for (size_t j = 0; j < df_charbuf_size; ++j) {
                                        tmp_tkeys[j] = uint8_t(cur_col[j]) ^ 0x80u;
                                    }
                                }

                            }

                    }

                    radix_sort_charbuf_mt<CORES, Simd>(tkeys.data(), 
                                                       nrow,
                                                       df_charbuf_size,
                                                       idx.data());

                } else if constexpr (CORES <= 1) {

                    if constexpr (Simd) {

                            #if defined (__AVX512F__)
                            char_to_u8buf2d_avx512<df_charbuf_size>(tkeys.data(), 
                                                         col, 
                                                         0,
                                                         nrow,
                                                         df_charbuf_size); 
                            #else
                            char_to_u8buf2d_avx2<df_charbuf_size>(tkeys.data(), 
                                                         col, 
                                                         0,
                                                         nrow,
                                                         df_charbuf_size); 
                            #endif


                    } else if constexpr (!Simd) {

                        for (size_t i = 0; i < nrow; ++i) {
                            const int8_t (&cur_col)[df_charbuf_size] = col[i];
                            std::vector<uint8_t[df_charbuf_size]>& tmp_tkeys = tkeys[i];
                            for (size_t j = 0; j < df_charbuf_size; ++j) {
                                tmp_tkeys[j] = uint8_t(cur_col[j]) ^ 0x80u;
                            }
                        }

                    }

                    radix_sort_charbuf<Simd>          (tkeys.data(), 
                                                       nrow,
                                                       df_charbuf_size,
                                                       idx.data());

                }

            }

    } else if constexpr (S == SortType::Standard) {

            if constexpr (CORES == 1) {

                    std::sort(idx.begin(), idx.end(), cmp);

            } else if constexpr (CORES > 1) {
                
                std::vector<std::pair<size_t, size_t>> chunks(CORES);
               
                // precomputation of the chunks
                for (size_t t = 0; t < CORES; t++) {
                    size_t start = nrow * t / CORES;
                    size_t end   = nrow * (t + 1) / CORES;
                    chunks[t] = {start, end};
                }
                
                // parallel sorting
                #pragma omp parallel num_threads(CORES)
                {
                    int tid = omp_get_thread_num();
                    auto [start, end] = chunks[tid];
                    std::sort(idx.begin() + start, idx.begin() + end, cmp);
                }

                std::vector<size_t> tmp(nrow);
                bool flip = false;
                
                while (chunks.size() > 1) {
                
                    std::vector<std::pair<size_t,size_t>> next;
                    next.resize(chunks.size() / 2);   
                
                    const size_t end_loop = chunks.size() - 1;

                    #pragma omp parallel for num_threads(CORES)
                    for (size_t i = 0; i < end_loop; i += 2) {
                        auto [s1, e1] = chunks[i];
                        auto [s2, e2] = chunks[i + 1];

                        size_t* A1;
                        size_t* A2;
                        size_t* B1;
                        size_t* B2;
                        size_t* Out;
                        
                        if (!flip) {
                            A1 = idx.data() + s1;
                            A2 = A1 + (e1 - s1);
                        
                            B1 = idx.data() + e1;
                            B2 = B1 + (e2 - e1);
                        
                            Out = tmp.data() + s1;
                        }
                        else {
                            A1 = tmp.data() + s1;
                            A2 = A1 + (e1 - s1);
                        
                            B1 = tmp.data() + e1;
                            B2 = B1 + (e2 - e1);
                        
                            Out = idx.data() + s1;
                        }
                        
                        std::merge(A1, A2, B1, B2, Out, cmp);
                
                        next[i / 2] = {s1, e2};
                    }
                
                    if (chunks.size() % 2 == 1) {
                        next.push_back(chunks.back());
                    }
                
                    chunks = std::move(next);
                    flip = !flip;
                }
                
                if (flip)
                    idx = tmp;

            }

    }

    if constexpr (!ASC) {
      std::reverse(idx.begin(), idx.end());
    }

}


