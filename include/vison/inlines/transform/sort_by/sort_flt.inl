#pragma once

template <bool ASC, 
    unsigned int CORES = 4, 
    bool Simd = true,
    SortType S = SortType::Radix,
    typename ComparatorFactory = DefaultComparatorFactory>
inline void sort_flt(
    std::vector<size_t>& idx,
    const FloatT* col,
    const size_t nrow,
    ComparatorFactory make_cmp = ComparatorFactory{}
)
{

    auto cmp = make_cmp.template operator()<ASC, FloatT>(col);
    static_assert(IndexComparator<decltype(cmp)>,
              "Comparator must be cmp(size_t,size_t)->bool");

    if constexpr (S == SortType::Radix) {

        if constexpr (std::is_same_v<FloatT, float>) {

            std::vector<float> tkeys(nrow);

            if constexpr (CORES == 1) {

                if constexpr (Simd) {

                    #if defined (__AVX512F__)
                    float_to_u32_avx512(tkeys.data(), 
                                        col,
                                        0,
                                        nrow);
                    #elif defined (__AVX2__)
                    float_to_u32_avx2(tkeys.data(), 
                                      col,
                                      0,
                                      nrow);
                    #endif

                } else if constexpr (!Simd) {

                    for (size_t i = 0; i < nrow; ++i) {
                        uint32_t bits;
                        memcpy(&bits, col + i, sizeof(bits));   

                        uint32_t mask = -(bits >> 31);     // 0xFFFFFFFF if negative, else 0
                        tkeys[i] = bits ^ (mask | 0x80000000u);
                    }

                }

                radix_sort_float<Simd>(tkeys.data(), idx.data(), nrow);

            } else if constexpr (CORES > 1) {

                if constexpr (Simd) {

                    #pragma omp parallel num_threads(CORES)
                    {

                        const size_t tid   = omp_get_thread_num();
                        const size_t chunk = nrow / CORES;
                        const size_t rem   = nrow % CORES;
    
                        const size_t start = tid * chunk + std::min(tid, rem);
                        const size_t end   = start + chunk + (tid < rem ? 1 : 0);

                        #if defined (__AVX512F__)
                        float_to_u32_avx512(tkeys.data(), 
                                            col,
                                            start,
                                            end);
                        #elif defined (__AVX2__)
                        float_to_u32_avx2(tkeys.data(), 
                                          col,
                                          start,
                                          end);
                        #endif

                    }

                } else if constexpr (!Simd) {

                    #pragma omp parallel for num_threads(CORES)
                    for (size_t i = 0; i < nrow; ++i) {
                        uint32_t bits;
                        memcpy(&bits, col + i, sizeof(bits));   

                        uint32_t mask = -(bits >> 31);     // 0xFFFFFFFF if negative, else 0
                        tkeys[i] = bits ^ (mask | 0x80000000u);
                    }

                }

                radix_sort_float_mt<CORES, Simd>(tkeys.data(), idx.data(), nrow);

            }

        } else if constexpr (std::is_same_v<FloatT, double>) {

            std::vector<double> tkeys(nrow);

            if constexpr (CORES == 1) {

                if constexpr (Simd) {

                    #if defined (__AVX512F__)
                    double_to_u32_avx512(tkeys.data(), 
                                         col,
                                         0,
                                         nrow);
                    #elif defined (__AVX2__)
                    double_to_u32_avx2(tkeys.data(), 
                                       col,
                                       0,
                                       nrow);
                    #endif

                } else if constexpr (!Simd) {

                    for (size_t i = 0; i < nrow; ++i) {
                        uint64_t bits;
                        memcpy(&bits, col + i, sizeof(bits));   

                        uint64_t mask = -(bits >> 63);     // 0xFFFFFFFF if negative, else 0
                        tkeys[i] = bits ^ (mask | 0x8000000000000000ULL);
                    }

                }

                radix_sort_double<Simd>(col, idx.data(), nrow);

            } else if constexpr (CORES > 1) {

                if constexpr (Simd) {

                    #pragma omp parallel num_threads(CORES)
                    {

                        const size_t tid   = omp_get_thread_num();
                        const size_t chunk = nrow / CORES;
                        const size_t rem   = nrow % CORES;
    
                        const size_t start = tid * chunk + std::min(tid, rem);
                        const size_t end   = start + chunk + (tid < rem ? 1 : 0);

                        #if defined (__AVX512F__)
                        double_to_u32_avx512(tkeys.data(), 
                                             col,
                                             start,
                                             end);
                        #elif defined (__AVX2__)
                        double_to_u32_avx2(tkeys.data(), 
                                           col,
                                           start,
                                           end);
                        #endif

                    }

                } else if constexpr (!Simd) {

                    #pragma omp parallel for num_threads(CORES)
                    for (size_t i = 0; i < nrow; ++i) {
                        uint64_t bits;
                        memcpy(&bits, col + i, sizeof(bits));   

                        uint64_t mask = -(bits >> 63);     // 0xFFFFFFFF if negative, else 0
                        tkeys[i] = bits ^ (mask | 0x8000000000000000ULL);
                    }

                }

                radix_sort_double_mt<CORES, Simd>(col, idx.data(), nrow);

            }
            
        } else {
            static_assert(std::is_same_v<FloatT, void>,
                "Unsupported IntT: must be int32_t or int64_t");
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


