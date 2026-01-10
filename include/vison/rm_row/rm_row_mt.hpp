#pragma once

template <unsigned int CORES = 4, 
          bool NUMA = false,
          bool MemClean = false,
          bool Soft = false>
void rm_row_mt(unsigned int x) 
{

    const size_t old_nrow = nrow;

    if constexpr (Soft) {

        if (!in_view) {
            in_view = true;
            row_view_idx.resize(nrow);
            std::iota(row_view_idx.begin(), row_view_idx.end(), 0);
        }
        row_view_idx.erase(row_view_idx.begin() + x);

    } else {

        auto eraser = [x](auto& matr, const size_t idx_type) {

            const size_t ncols_cur = matr_idx[idx_type].size();

            if constexpr (CORES > 1) {

                int numa_nodes = 1;
                if (numa_available() >= 0) 
                    numa_nodes = numa_max_node() + 1;

                #pragma omp parallel num_threads(CORES)
                {

                    const int tid        = omp_get_thread_num();
                    const int nthreads   = omp_get_num_threads();
           
                    MtStruct cur_struct;

                    if constexpr (NUMA) {
                        numa_mt(cur_struct,
                                ncols_cur, 
                                tid, 
                                nthreads, 
                                numa_nodes);
                    } else {
                        simple_mt(cur_struct,
                                  ncols_cur, 
                                  tid, 
                                  nthreads);
                    }
                        
                    const unsigned int start = cur_struct.start;
                    const unsigned int end   = cur_struct.end;

                    for (size_t cpos = start; cpos < end; ++cpos) 
                        matr[cpos].erase(matr[cpos].begin() + x);

                }

            } else {

                for (size_t cpos = 0; cpos < ncols_cur; ++cpos) 
                    matr[cpos].erase(matr[cpos].begin() + x);

            }

        };

        eraser(str_v,  0);
        eraser(chr_v,  1);
        eraser(bool_v, 2);
        eraser(int_v,  3);
        eraser(uint_v, 4);
        eraser(dbl_v,  5);

        if (!name_v_row.empty()) {
            name_v_row.erase(name_v_row.begin() + x);
            if constexpr (MemClean) {
              name_v_row.shrink_to_fit();
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

    nrow = old_nrow - 1; 

};






