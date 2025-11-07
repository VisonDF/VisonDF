#pragma once

template <bool MemClean = false>
void rm_row(unsigned int x) 
{

    const size_t old_nrow = nrow;

    for (size_t t = 0; t < 6; ++t) {
        
        const std::vector<unsigned int>& matr_tmp = matr_idx[t];

        if (matr_tmp.empty()) {
            continue;
        }

        const size_t ncols_t = matr_tmp.size();
        
        for (size_t cpos = ncols_t; cpos-- > 0; ) {
            
            const size_t base = cpos * old_nrow;
            
            switch (t) {
                case 0: str_v.erase(str_v.begin() + x); break;
                case 1: chr_v.erase(chr_v.begin() + x); break;
                case 2: bool_v.erase(bool_v.begin() + x); break;
                case 3: int_v.erase(int_v.begin() + x); break;
                case 4: uint_v.erase(uint_v.begin() + x); break;
                case 5: dbl_v.erase(dbl_v.begin() + x); break;
            }
            
            auto& aux = tmp_val_refv[matr_tmp[cpos]];
            aux.erase(aux.begin() + x);
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
        name_v_row.erase(name_v_row.begin() + x);
        if constexpr (MemClean) {
          name_v_row.shrink_to_fit();
        }
    }

};




