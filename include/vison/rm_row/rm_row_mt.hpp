#pragma once

template <unsigned int CORES = 4, 
          bool MemClean = false,
          bool Soft = false>
void rm_row_mt(unsigned int x) 
{

    const size_t old_nrow = nrow;

    for (size_t t = 0; t < 6; ++t) {
        
        const std::vector<unsigned int>& matr_tmp = matr_idx[t];

        if (matr_tmp.empty()) {
            continue;
        }

        const size_t ncols_t = matr_tmp.size();
        
        switch (t) {
            case 0: {
                        #pragma omp parallel for num_threads(CORES)
                        for (size_t cpos = 0; cpos < ncols_t; ++cpos) 
                            str_v[cpos].erase(str_v[cpos].begin() + x);

                        break; 
                    }
            case 1: {   
                        #pragma omp parallel for num_threads(CORES)
                        for (size_t cpos = 0; cpos < ncols_t; ++cpos) 
                            chr_v[cpos].erase(chr_v[cpos].begin() + x);

                        break;
                    }
            case 2: {
                        #pragma omp parallel for num_threads(CORES)
                        for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                            bool_v[cpos].erase(bool_v[cpos].begin() + x);

                        break; 
                    }
            case 3: {
                        #pragma omp parallel for num_threads(CORES)
                        for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                            int_v[cpos].erase(int_v[cpos].begin() + x);

                        break; 
                    }
            case 4: {
                        #pragma omp parallel for num_threads(CORES)
                        for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                            uint_v[cpos].erase(uint_v[cpos].begin() + x); 

                        break; 
                    }
            case 5: {
                        #pragma omp parallel for num_threads(CORES)
                        for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                            dbl_v[cpos].erase(dbl_v[cpos].begin() + x); 
                    
                        break; 
                    }
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

    nrow = old_nrow - 1; 

    if (!name_v_row.empty()) {
        name_v_row.erase(name_v_row.begin() + x);
        if constexpr (MemClean) {
          name_v_row.shrink_to_fit();
        }
    }

};




