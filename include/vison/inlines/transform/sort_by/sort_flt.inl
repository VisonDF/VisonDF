#pragma once

template <bool ASC, 
    unsigned int CORES = 4, 
    bool Simd = true,
    SortType S = SortType::Radix,
    Comparator Cmp = std::less<size_t>>
inline void sort_flt(
    std::vector<size_t>& idx,
    unsigned int nrow,
    unsigned int col_id,
    Cmp cmp = Cmp{}
)
{

    const FloatT* col = int_v.data() + col_id * nrow;

    if constexpr (S == SortType::Radix) {

        if constexpr (std::is_same_v<FloatT, float>) {

            if constexpr (CORES == 1) {

                radix_sort_float<Simd>(col, idx.data(), nrow);

            } else if constexpr (CORES > 1) {

                radix_sort_float_mt<CORES, Simd>(col, idx.data(), nrow);

            }

        } else if constexpr (std::is_same_v<FloatT, double>) {

            if constexpr (CORES == 1) {

                radix_sort_double<Simd>(col, idx.data(), nrow);

            } else if constexpr (CORES > 1) {

                radix_sort_double_mt<CORES, Simd>(col, idx.data(), nrow);

            }
            
        } else {
            static_assert(std::is_same_v<FloatT, void>,
                "Unsupported IntT: must be int32_t or int64_t");
        }

    } else if constexpr (S == SortType::Standard) {

            if constexpr (CORES == 1) {

                    std::sort(col.begin(), col.end(), cmp);

            } else if constexpr (CORES > 1) {
                
                std::vector<std::pair<size_t, size_t>> chunks(CORES);
               
                // precomputation of the chunks
                for (size_t t = 0; t < CORES; t++) {
                    size_t start = n * t / CORES;
                    size_t end   = n * (t + 1) / CORES;
                    chunks[t] = {start, end};
                }
                
                // parallel sorting
                #pragma omp parallel num_threads(CORES)
                {
                    int tid = omp_get_thread_num();
                    auto [start, end] = chunks[tid];
                    std::sort(col.begin() + start, col.begin() + end, cmp);
                }

                std::vector<int> tmp(n);
                bool flip = false;
                
                while (chunks.size() > 1) {
                
                    std::vector<std::pair<size_t,size_t>> next;
                    next.resize(chunks.size() / 2);   
                
                    #pragma omp parallel for
                    for (size_t i = 0; i + 1 < chunks.size(); i += 2) {
                        auto [s1, e1] = chunks[i];
                        auto [s2, e2] = chunks[i + 1];
                
                        if (!flip) {
                            parallel_merge<CORES>(&col[s1], e1 - s1,
                                                  &col[e1], e2 - e1,
                                                  &tmp[s1]);
                        } else {
                            parallel_merge<CORES>(&tmp[s1], e1 - s1,
                                                  &tmp[e1], e2 - e1,
                                                  &col[s1]);
                        }
                
                        next[i/2] = {s1, e2};
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

    if constexpr (!ASC) {
      std::reverse(idx.begin(), idx.end());
    }

}


