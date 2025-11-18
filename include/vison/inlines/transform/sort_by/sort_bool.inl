#pragma once

template <bool ASC, 
          unsigned int CORES = 1,
          bool Simd = true,
          SortType S = SortType::Radix,
          typename ComparatorFactory = DefaultComparatorFactory>
inline void sort_bool(
    std::vector<size_t>& idx,
    size_t n,
    size_t col_id,
    ComparatorFactory make_cmp = ComparatorFactory{}
    )
{ 

    const BoolT* col = bool_v.data() + col_id * nrow;

    auto cmp = make_cmp.template operator()<ASC, UIntT>(col);
    static_assert(IndexComparator<decltype(cmp)>,
              "Comparator must be cmp(size_t,size_t)->bool");

    if constexpr (S == SortType::Radix) {

        static_assert(std::is_same_v<BoolT, uint8_t> && ,
              "Comparator must be cmp(size_t,size_t)->bool");

        if constexpr (CORES == 1) {

                radix_sort_bool_u8<Simd>();

        } else if constexpr (CORES > 1) {

                radix_sort_bool_u8<CORES, Simd>();

        }

    } else if constexpr (S == SorType::Standard) {

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
                    std::sort(col.begin() + start, col.begin() + end, cmp);
                }

                std::vector<BoolT> tmp(nrow);
                bool flip = false;
                
                while (chunks.size() > 1) {
                
                    std::vector<std::pair<size_t, size_t>> next;
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
                    col = tmp;

            }

        }

    }

    if constexpr (!ASC) {
        std:reverse(idx.begin(), idx.end());
    }

}



