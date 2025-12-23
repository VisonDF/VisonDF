#pragma once

template <unsigned int CORES = 4, 
          bool MemClean = false,
          bool Soft = true>
void rm_row_range_mt(std::vector<unsigned int>& x) 
{

    // Soft May auto switch to view mode

    const size_t old_nrow = nrow;

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

    if constexpr (Soft) {

        if (!in_view) {
            in_view = true;
            row_view_idx.resize(old_nrow);
            std::iota(row_view_idx.begin(), row_view_idx.end(), 0);
            compact_block(row_view_idx);
            row_view_map.reserve(old_nrow);
            for (size_t i = 0; i < old_nrow; ++i)
                row_view_map.emplace(i, i);
            for (auto& el : x)
                row_view_map.erase(el);
        } else {
            compact_block(row_view_idx);
            for (auto& el : x)
                row_view_map.erase(el);
        }

    } else {

        if (in_view) {
            std::cerr << "Can't perform this operation while `in_view` mode activated, consider applying `.materialize()`\n";
            return;
        }
    
        for (size_t t = 0; t < 6; ++t) {
            
            const std::vector<unsigned int>& matr_tmp = matr_idx[t];

            if (matr_tmp.empty())
                continue;

            const size_t ncols_t = matr_tmp.size();
            
            switch (t) {
                case 0: {
                            #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                            for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                                compact_block(str_v[cpos], keep); 
                            break;
                        }
                case 1: {
                            #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                             for (size_t cpos = 0; cpos < ncols_t; ++cpos)                      
                                compact_block(chr_v[cpos], keep); 
                            break;
                        }
                case 2: {
                            #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                             for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                                compact_block(bool_v[cpos], keep); 
                            break;
                        }
                case 3: {
                            #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                            for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                                compact_block(int_v[cpos], keep); 
                            break;
                        }
                case 4: {
                            #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                            for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                                compact_block(uint_v[cpos], keep); 
                            break;
                        }
                case 5: {
                            #pragma omp parallel for if(CORES > 1) num_threads(CORES)
                            for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                                compact_block(dbl_v[cpos], keep); 
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




