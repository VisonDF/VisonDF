#pragma once

template <unsigned int CORES = 4, 
          bool NUMA = false,
          bool MemClean = false,
          bool Soft = false,
          AssertionType AssertionLevel = AssertionType::Simple
         >
void rm_row_range_mt(const unsigned int start,
                     const unsigned int end) 
{

    if constexpr (AssertionLevel > AssertionType::None) {
        if (!(end > start)) {
            std::runtime_error("end must be strictly superior to start\n");
        }
        if (end > nrow) {
            std::runtime_error("end out of bounds\n");
        }
    }

    const size_t old_nrow = nrow;

    if constexpr (Soft) {

        if (!in_view) {
            in_view = true;
            row_view_idx.resize(nrow);
            std::iota(row_view_idx.begin(), row_view_idx.end(), 0);
        }

        memmove(row_view_idx.data() + start, 
                row_view_idx.data() + end, 
                end - start);

    } else {

        const unsigned int len = end - start;

        auto eraser = [len,
                       x](auto& matr, const size_t idx_type) {

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

                    for (size_t cpos = start; cpos < end; ++cpos) {
                        memmove(matr[cpos].data() + start,
                                matr[cpos].data() + end,
                                len);
                    }

                }

            } else {

                for (size_t cpos = 0; cpos < ncols_cur; ++cpos) {
                    memmove(matr[cpos].data() + start,
                    matr[cpos].data() + end,
                    len);
                }

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
                name_v.resize(len);
                name_v_row.shrink_to_fit();
            }
        }

        if constexpr (MemClean) {
            for (auto& el : str_v)  el.resize(len); el.shrink_to_fit();
            for (auto& el : chr_v)  el.resize(len); el.shrink_to_fit();
            for (auto& el : bool_v) el.resize(len); el.shrink_to_fit();
            for (auto& el : int_v)  el.resize(len); el.shrink_to_fit();
            for (auto& el : uint_v) el.resize(len); el.shrink_to_fit();
            for (auto& el : dbl_v)  el.resize(len); el.shrink_to_fit();
        }

    }

    nrow = old_nrow - 1; 

};






