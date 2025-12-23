#pragma once

template <unsigned int CORES = 4,
          bool MemClean = false,
          bool Soft = true>
void rm_row_range_reconstruct_boolmask_mt(std::vector<uint8_t>& x,
                                          const size_t strt_vl)
{

    const size_t old_nrow = nrow;
    if (x.empty() || old_nrow == 0) return;

    const size_t new_nrow = std::count(x.begin(), x.end(), 0);
    if (new_nrow == nrow) return;
    if (new_nrow == 0) {
        std::cout << "Consider using .empty() if you want to remove all rows\n"; 
        return;
    }

    if constexpr (Soft) {

        if (!in_view) {
            row_view_idx.resize(old_nrow);
            std::iota(row_view_idx.begin(), row_view_idx.end(), 0);
            for (size_t i = 0; i < old_nrow; ++i)
                row_view_map.emplace(i, i);
            in_view = true;
        }
            
        size_t i = 0;
        size_t written = 0;
        while (!x[i]) {
            i += 1;
            written += 1;
        }
        while (i < new_nrow && x[i]) {
            i += 1;
        }
        while (i < x.size()) {
            size_t start = i;
            while (i < x.size() && !x[i]) ++i;
            size_t len = i - start; 
            {
                T* __restrict d = row_view_idx.data() + written;
                T* __restrict s = row_view_idx.data() + start;
                
                std::memmove(d, s, len * sizeof(size_t));
            } 
            written += len;
            i += 1;
        }

    } else {

        if (in_view) {
            std::cerr << "Can't perform this operation while `in_view` mode activated, consider applying `.materialize()`\n";
            return;
        }

        auto compact_block_pod = [&]<typename T>(std::vector<T>& dst, 
                                                 std::vector<T>& src) {

            size_t i = 0;
            size_t written = 0;
            while (!x[i]) {
                i += 1;
                written += 1;
            }
            while (i < new_nrow && x[i]) {
                i += 1;
            }
            while (i < x.size()) {
            
                size_t start = i;
                while (i < x.size() && !x[i]) ++i;
            
                size_t len = i - start;
                {
                    T* __restrict d = dst.data() + written;
                    T* __restrict s = src.data() + strt_vl + start;
           
                    memmove(d, s, len * sizeof(T))

                }
            
                written += len;
                i += 1;
            }
        };

        auto compact_block_scalar = [&](auto& dst, 
                                        auto& src) {
            size_t i = 0;
            size_t written = 0;
            while (!x[i]) {
                i += 1;
                written += 1;
            }
            while (i < new_nrow && x[i]) {
                i += 1;
            }
            while (i < x.size()) {
                while (i < x.size() && !x[i]) {
                    dst[written] = std::move(src[i]);
                    i += 1;
                    written += 1;
                };
                i += 1;
            }
        };

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
        
        if (!name_v_row.empty()) {
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


