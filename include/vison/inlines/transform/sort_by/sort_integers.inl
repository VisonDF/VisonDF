#pragma once

template <bool ASC, 
        unsigned int CORES = 4, 
        bool Simd = true,
        SortType S = SortType::Radix,
        typename ComparatorFactory = DefaultComparatorFactory>
inline void sort_integers(
    std::vector<size_t>& idx,
    const IntT* col,
    const size_t nrow,
    ComparatorFactory make_cmp = ComparatorFactory{}
)
{

    auto cmp = make_cmp.template operator()<ASC, IntT>(col);
    static_assert(IndexComparator<decltype(cmp)>,
              "Comparator must be cmp(size_t,size_t)->bool");

    std::vector<UIntT> tkeys(nrow);

    auto dispatch = [&](size_t nrow, UIntT c_byte)
    {

        auto process_range = [&](size_t start, size_t end)
        {
            if constexpr (Simd)
            {
            #if defined(__AVX512F__)
                int_to_uint_avx512(tkeys.data(),
                                   col,
                                   start,
                                   end,
                                   c_byte);
    
            #elif defined(__AVX2__)
                int_to_uint_avx2(tkeys.data(),
                                 col,
                                 start,
                                 end,
                                 c_byte);
    
            #else
                #pragma unroll
                for (size_t i = start; i < end; ++i)
                    tkeys[i] = UIntT(col[i]) ^ c_byte;
            #endif
            }
            else
            {
                #pragma unroll
                for (size_t i = start; i < end; ++i)
                    tkeys[i] = UIntT(col[i]) ^ c_byte;
            }
        };
    
        if constexpr (CORES > 1)
        {
            #pragma omp parallel num_threads(CORES)
            {
                size_t tid   = omp_get_thread_num();
                size_t chunk = nrow / CORES;
                size_t rem   = nrow % CORES;
    
                size_t start = tid * chunk + std::min(tid, rem);
                size_t end   = start + chunk + (tid < rem ? 1 : 0);
    
                process_range(start, end);
            }
        }
        else
        {
            process_range(0, nrow);
        }
    };

    if constexpr (S == SortType::Radix) {

        if constexpr (std::is_same_v<IntT, int8_t>) {

            dispatch(nrow, 0x80u);

            if constexpr (CORES == 1) {

                radix_sort_uint8<Simd>          (tkeys.data(), idx.data(), nrow);

            } else if constexpr (CORES > 1) {

                radix_sort_uint8_mt<CORES, Simd>(tkeys.data(), idx.data(), nrow);

            }

        } else if constexpr (std::is_same_v<IntT, int16_t>) {

            dispatch(nrow, 0x8000u);

            if constexpr (CORES == 1) {

                radix_sort_uint16<Simd>          (tkeys.data(), idx.data(), nrow);

            } else if constexpr (CORES > 1) {

                radix_sort_uint16_mt<CORES, Simd>(tkeys.data(), idx.data(), nrow);

            }

        } else if constexpr (std::is_same_v<IntT, int32_t>) {

            dispatch(nrow, 0x80000000u);

            if constexpr (CORES == 1) {

                radix_sort_uint32<Simd>          (tkeys, idx.data(), nrow);

            } else if constexpr (CORES > 1) {

                radix_sort_uint32_mt<CORES, Simd>(tkeys, idx.data(), nrow);

            }

        } else if constexpr (std::is_same_v<IntT, int64_t>) {

            dispatch(nrow, 0x8000000000000000ULL);

            if constexpr (CORES == 1) {

                radix_sort_uint64<Simd>          (tkeys, idx.data(), nrow);

            } else if constexpr (CORES > 1) {

                radix_sort_uint64_mt<CORES, Simd>(tkeys, idx.data(), nrow);

            }
            
        } else {
            static_assert(std::is_same_v<IntT, void>,
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


