#pragma once

template <bool Sorted = true,
          unsigned int CORES = 4, 
          bool MemClean = false,
          bool Soft = true,
          bool SanityCheck = true>
void rm_row_range_mt(std::vector<unsigned int>& x) 
{

    const size_t old_nrow = nrow;

    if constexpr (Soft) {

        in_view = true;
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
        }

    } else {

        if (in_view) {
            std::cerr << "Can't perform this operation while `in_view` mode activated, consider applying `.materialize()`\n";
            return;
        }

        std::vector<uint8_t> keep(old_nrow, 1);
        for (unsigned int& rr : x) {
            if (rr < old_nrow) [[likely]] {
                keep[rr] = 0;
            } else {
                std::cerr << "Row out of bounds in (rm_row_range_reconstruct_mt)\n";
                return;
            }
        }

        auto compact_block = [&](auto& vec) {
            size_t idx = 0;
            auto beg = vec.begin();
            auto end = beg + old_nrow;
            auto it  = std::remove_if(beg, end, [&](auto&) mutable { return !keep[idx++]; });
            vec.erase(it, end);
        };

        for (size_t t = 0; t < 6; ++t) {
            
            const std::vector<unsigned int>& matr_tmp = matr_idx[t];

            if (matr_tmp.empty())
                continue;

            const size_t ncols_t = matr_tmp.size();
            
            switch (t) {
                case 0: {
                            #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                            for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                                compact_block(str_v[cpos]); 
                            break;
                        }
                case 1: {
                            #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                             for (size_t cpos = 0; cpos < ncols_t; ++cpos)                      
                                compact_block(chr_v[cpos]); 
                            break;
                        }
                case 2: {
                            #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                             for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                                compact_block(bool_v[cpos]); 
                            break;
                        }
                case 3: {
                            #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                            for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                                compact_block(int_v[cpos]); 
                            break;
                        }
                case 4: {
                            #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                            for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                                compact_block(uint_v[cpos]); 
                            break;
                        }
                case 5: {
                            #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                            for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                                compact_block(dbl_v[cpos]); 
                            break;
                        }
            }
                        
        }

        if (!name_v_row.empty()) {
            auto& aux = name_v_row;
            size_t idx = 0;
            auto it = std::remove_if(aux.begin(), aux.end(),
                                     [&](auto&) mutable { return !keep[idx++]; });
            aux.erase(it, aux.end());
            if constexpr (MemClean) {
               aux.shrink_to_fit();
            }
        }

        if constexpr (MemClean) {
            for (auto& el : str_v)  el.shrink_to_fit();
            for (auto& el : chr_v)  el.shrink_to_fit();
            for (auto& el : bool_v) el.shrink_to_fit();
            for (auto& el : int_v)  el.shrink_to_fit();
            for (auto& el : uint_v) el.shrink_to_fit();
            for (auto& el : dbl_v)  el.shrink_to_fit();
        }

    }

    nrow = old_nrow - x.size(); 

};




