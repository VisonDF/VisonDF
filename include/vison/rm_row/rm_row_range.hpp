#pragma once

template <bool MemClean = false>
void rm_row_range(std::vector<unsigned int>& x) 
{

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

    for (size_t t = 0; t < 6; ++t) {
        
        const std::vector<unsigned int>& matr_tmp = matr_idx[t];

        if (matr_tmp.empty()) {
            continue;
        }

        const size_t ncols_t = matr_tmp.size();
        
        switch (t) {
            case 0: { 
                        for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                            compact_block(str_v[cpos]); 
                        break;
                    }
            case 1: {
                         for (size_t cpos = 0; cpos < ncols_t; ++cpos)                      
                            compact_block(chr_v[cpos]); 
                        break;
                    }
            case 2: {
                         for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                            compact_block(bool_v[cpos]); 
                        break;
                    }
            case 3: {
                        for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                            compact_block(int_v[cpos]); 
                        break;
                    }
            case 4: {
                        for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                            compact_block(uint_v[cpos]); 
                        break;
                    }
            case 5: {
                        for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                            compact_block(dbl_v[cpos]); 
                        break;
                    }
        }
                    
    }

    for (auto& el : tmp_val_refv) {
        size_t idx = 0;
        auto it = std::remove_if(el.begin(), el.end(),
                                 [&](auto&) mutable { return !keep[idx++]; });
        el.erase(it, aux.end()); 
    }

    if constexpr (MemClean) {
        for (auto& el : str_v) 
            el.shrink_to_fit();
        for (auto& el : chr_v) 
            el.shrink_to_fit();
        for (auto& el : bool_v) 
            el.shrink_to_fit();
        for (auto& el : int_v) 
            el.shrink_to_fit();
        for (auto& el : uint_v) 
            el.shrink_to_fit();
        for (auto& el : dbl_v) 
            el.shrink_to_fit();
        for (auto& el : tmp_val_refv)
            el.shrink_to_fit();
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

    nrow = old_nrow - x.size(); 

};




