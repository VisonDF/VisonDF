#pragma once

template <unsigned int CORES = 4,
          bool Sorted = true,
          bool MemClean = false,
          bool Soft = true,
          bool SanityCheck = true>
void rm_row_range_reconstruct_mt(std::vector<unsigned int>& x)
{

    const size_t old_nrow = nrow;
    if (x.empty() || old_nrow == 0) return;
    if constexpr (!Sorted) std::sort(x.begin(), x.end());

    const size_t new_nrow = old_nrow - x.size();

    if constexpr (Soft) {

        if (row_view_idx.empty())
            std::iota(row_view_idx.begin(), row_view_idx.end(), 0);

        bool sort_assumption = Sorted;

        if constexpr (!Sorted) {
            if (x.size() > 1000) {
                std::vector<uint8_t> is_active(nrow, 1);
                for (auto& i : x) {
                    if (i < old_nrow) [[likely]] {
                        is_active[i] = 0;
                    }
                }
                size_t i2 = 0;
                for (size_t i = 0; i < old_nrow; ++i) {
                    if (is_active[i]) {
                        row_view_idx[i2] = row_view_idx[i];
                        i2 += 1;
                    }
                }
            } else {
                std::sort(x.begin(), x.end());
                if constexpr (SanityCheck) {
                    x.erase(
                        std::remove_if(x.begin(), x.end(),
                                       [&](size_t v){ return v >= old_nrow; }),
                        x.end()
                    );
                    x.erase(std::unique(x.begin(), x.end()), x.end());
                }
                sort_assumption = true;
            }
        } 

        if (sort_assumption) {
            size_t i = x[0] + 1;
            size_t i2 = 1;
            size_t written = x[0];
            while (i2 < x.size()) {
                unsigned ref_val = x[i2++];
                const size_t start = i;
                while (i < ref_val) ++i;
                size_t len = i - start;
                {
                    T* __restrict d = row_view_idx.data() + written;
                    T* __restrict s = row_view_idx.data() + start;
                    
                    std::memmove(d, s, len * sizeof(size_t));
                } 
                written += len;
                i += 1;
            }
            while (i < old_nrow) {
                row_view_idx[written] = row_view_idx[i];
                i += 1;
                written += 1;
            };

    } else {

        auto compact_block_pod_view = [&]<typename T>(std::vector<T>& dst, 
                                                      std::vector<T>& src) {

            size_t i = x[0] + 1;
            size_t i2 = 1;
            size_t written = x[0];
            while (i2 < x.size()) {
            
                unsigned ref_val = x[i2++];
                size_t start = i;
                while (i < ref_val) ++i;
            
                size_t len = i - start;
                #pragma GCC ivdep
                for (size_t k = 0; k < len; ++k)
                    dst[written + k] = src[row_view_idx[start + k]];
            
                written += len;
                i += 1;
            }
            while (i < old_nrow) {
                dst[row_view_idx[written]] = src[row_view_idx[i]];
                i += 1;
                written += 1;
            };

        }

        auto compact_block_pod = [&]<typename T>(std::vector<T>& dst, 
                                                 std::vector<T>& src) {

            size_t i = x[0] + 1;
            size_t i2 = 1;
            size_t written = x[0];
            while (i2 < x.size()) {
            
                unsigned ref_val = x[i2++];
                size_t start = i;
                while (i < ref_val) ++i;
            
                size_t len = i - start;
                {
                    T* __restrict d = dst.data() + written;
                    T* __restrict s = src.data() + start;
           
                    memmove(d, s, len * sizeof(T))
                }
            
                written += len;
                i += 1;
            }
            while (i < old_nrow) {
                dst[written] = src[i];
                i += 1;
                written += 1;
            };
        };

        auto compact_block_scalar_view = [&](auto& dst, 
                                             auto& src) {
            size_t i = x[0] + 1;
            size_t i2 = 0;
            size_t written = x[0];
            while (i2 < x.size()) {
                const unsigned int ref_val = x[i2++];
                while (i < ref_val) {
                    dst[row_view_idx[written]] = std::move(src[row_view_idx[i]]);
                    i += 1;
                    written += 1;
                };
                i += 1;
            }
            while (i < old_nrow) {
                dst[row_view_idx[written]] = std::move(src[row_view_idx[i]]);
                i += 1;
                written += 1;
            };
        };

        auto compact_block_scalar = [&](auto& dst, 
                                        auto& src) {
            size_t i = x[0] + 1;
            size_t i2 = 0;
            size_t written = x[0];
            while (i2 < x.size()) {
                const unsigned int ref_val = x[i2++];
                while (i < ref_val) {
                    dst[written] = std::move(src[i]);
                    i += 1;
                    written += 1;
                };
                i += 1;
            }
            while (i < old_nrow) {
                dst[written] = std::move(src[i]);
                i += 1;
                written += 1;
            };
        };

        if (in_view) {
            for (size_t t = 0; t < 6; ++t) {
                
                const auto& idx = matr_idx[t];
                const size_t ncols_t = idx.size();
                if (ncols_t == 0) continue;

                switch (t) {
                    case 0: 
                        #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                        for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                            compact_block_scalar_view(str_v[cpos], str_v[cpos]);
                        break;
                    case 1:
                        #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                        for (size_t cpos = 0; cpos < ncols_t; ++cpos) {
                            compact_block_pod_view.template operator()<CharT>(chr_v[cpos],  
                                                                              chr_v[cpos]);
                        }
                        break;
                    case 2: 
                        #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                        for (size_t cpos = 0; cpos < ncols_t; ++cpos) {
                            compact_block_pod_view.template operator()<uint8_t>(bool_v[cpos],  
                                                                                bool_v[cpos]);
                        }
                        break;
                    case 3:
                        #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                        for (size_t cpos = 0; cpos < ncols_t; ++cpos) {
                            compact_block_pod_view.template operator()<IntT>(int_v[cpos], 
                                                                             int_v[cpos]);
                        }
                        break;
                    case 4:
                        #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                        for (size_t cpos = 0; cpos < ncols_t; ++cpos) {
                            compact_block_pod_view.template operator()<UIntT>(uint_v[cpos], 
                                                                              uint_v[cpos]);
                        }
                        break;
                    case 5:
                        #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                        for (size_t cpos = 0; cpos < ncols_t; ++cpos) {
                            compact_block_pod_view.template operator()<FloatT>(dbl_v[cpos],
                                                                               dbl_v[cpos]);
                        }
                        break;
                }
        } else {
            for (size_t t = 0; t < 6; ++t) {
                
                const auto& idx = matr_idx[t];
                const size_t ncols_t = idx.size();
                if (ncols_t == 0) continue;

                switch (t) {
                    case 0: 
                        #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                        for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                            compact_block_scalar(str_v[cpos], str_v[cpos]);
                        break;
                    case 1:
                        #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                        for (size_t cpos = 0; cpos < ncols_t; ++cpos) {
                            compact_block_pod.template operator()<CharT>(chr_v[cpos],  
                                                                         chr_v[cpos]);
                        }
                        break;
                    case 2: 
                        #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                        for (size_t cpos = 0; cpos < ncols_t; ++cpos) {
                            compact_block_pod.template operator()<uint8_t>(bool_v[cpos],  
                                                                           bool_v[cpos]);
                        }
                        break;
                    case 3:
                        #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                        for (size_t cpos = 0; cpos < ncols_t; ++cpos) {
                            compact_block_pod.template operator()<IntT>(int_v[cpos], 
                                                                        int_v[cpos]);
                        }
                        break;
                    case 4:
                        #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                        for (size_t cpos = 0; cpos < ncols_t; ++cpos) {
                            compact_block_pod.template operator()<UIntT>(uint_v[cpos], 
                                                                         uint_v[cpos]);
                        }
                        break;
                    case 5:
                        #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                        for (size_t cpos = 0; cpos < ncols_t; ++cpos) {
                            compact_block_pod.template operator()<FloatT>(dbl_v[cpos],
                                                                          dbl_v[cpos]);
                        }
                        break;
                }
            }
        }

        if (!name_v_row.empty()) {
            if (in_view) {
                size_t i = x[0] + 1;
                size_t i2 = 0;
                size_t written = x[0];
                while (i2 < x.size()) {
                    const unsigned int ref_val = x[i2++];
                    while (i < ref_val) {
                        name_v_row[row_view_idx[written]] = std::move(name_v_row[row_view_idx[i]]);
                        i += 1;
                        written += 1;
                    };
                    i += 1;
                }
                while (i < old_nrow) {
                    name_v_row[row_view_idx[written]] = std::move(name_v_row[row_view_idx[i]]);
                    i += 1;
                    written += 1;
                };
            } else {
                size_t i = x[0] + 1;
                size_t i2 = 0;
                size_t written = x[0];
                while (i2 < x.size()) {
                    const unsigned int ref_val = x[i2++];
                    while (i < ref_val) {
                        name_v_row[written] = std::move(name_v_row[i]);
                        i += 1;
                        written += 1;
                    };
                    i += 1;
                }
                while (i < old_nrow) {
                    name_v_row[written] = std::move(name_v_row[i]);
                    i += 1;
                    written += 1;
                };
            }
            if constexpr (MemClean) {
                name_v_row.resize(new_nrow);
                name_v_row.shrink_to_fit();
            }
        }

        if constexpr (MemClean) {
            for (auto& el : str_v)        el.resize(new_nrow); el.shrink_to_fit();
            for (auto& el : chr_v)        el.resize(new_nrow); el.shrink_to_fit();
            for (auto& el : bool_v)       el.resize(new_nrow); el.shrink_to_fit();
            for (auto& el : int_v)        el.resize(new_nrow); el.shrink_to_fit();
            for (auto& el : uint_v)       el.resize(new_nrow); el.shrink_to_fit();
            for (auto& el : dbl_v)        el.resize(new_nrow); el.shrink_to_fit();
        }

    }

    nrow = new_nrow;

}


