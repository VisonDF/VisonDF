#pragma once

template <bool MemClean = false>
void rm_row(unsigned int x) 
{

    const size_t old_nrow = nrow;

    auto compact_block = [&](auto& vec, size_t base) {
        size_t idx = 0;
        auto beg = vec.begin() + base;
        auto end = beg + old_nrow;
        auto it  = std::remove_if(beg, end, [&](auto&) mutable { return idx++ == x; });
        vec.erase(it, end);
    };

    for (size_t t = 0; t < 6; ++t) {
        
        const std::vector<unsigned int>& matr_tmp = matr_idx[t];

        if (matr_tmp.empty()) {
            continue;
        }

        const size_t ncols_t = matr_tmp.size();
        
        for (size_t cpos = ncols_t; cpos-- > 0; ) {
            
            const size_t base = cpos * old_nrow;
            
            switch (t) {
                case 0: compact_block(str_v,  base); break;
                case 1: compact_block(chr_v,  base); break;
                case 2: compact_block(bool_v, base); break;
                case 3: compact_block(int_v,  base); break;
                case 4: compact_block(uint_v, base); break;
                case 5: compact_block(dbl_v,  base); break;
            }
            
            auto& aux = tmp_val_refv[matr_tmp[cpos]];
            size_t idx = 0;
            auto it = std::remove_if(aux.begin(), aux.end(),
                                     [&](auto&) mutable { return idx++ == x; });
            aux.erase(it, aux.end());
            if constexpr (MemClean) {
              aux.shrink_to_fit();
            }
        }

        if constexpr (MemClean) {
            switch (t) {
                case 0: str_v.shrink_to_fit(); break;
                case 1: chr_v.shrink_to_fit(); break;
                case 2: bool_v.shrink_to_fit(); break;
                case 3: int_v.shrink_to_fit(); break;
                case 4: uint_v.shrink_to_fit(); break;
                case 5: dbl_v.shrink_to_fit(); break;
            }
        }

    }

    nrow = old_nrow - 1; 

    if (!name_v_row.empty()) {
        auto& aux = name_v_row;
        size_t idx = 0;
        auto it = std::remove_if(aux.begin(), aux.end(),
                                 [&](auto&) mutable { return idx++ == x; });
        aux.erase(it, aux.end());
        if constexpr (MemClean) {
          aux.shrink_to_fit();
        }
    }

};




