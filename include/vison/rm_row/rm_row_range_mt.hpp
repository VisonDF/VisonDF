#pragma once

template <unsigned int CORES = 4,
          bool NUMA = false,
          bool MemClean = false,
          bool Soft = true,
          bool Sorted = false,  // if not will affect x
          bool SanityCheck = false
         >
void rm_row_range_mt(std::vector<unsigned int>& x) 
{

    const size_t old_nrow = nrow;

    if constexpr (!Sorted) {
        std::sort(x.begin(), x.end());
    }
    if constexpr (SanityCheck) {
        x.erase(
            std::remove_if(x.begin(), x.end(),
                           [&](size_t v){ return v >= old_nrow; }),
            x.end()
        );
        x.erase(std::unique(x.begin(), x.end()), x.end());
    }

    auto compact_block = [&x](auto& vec) 
    {
        for (int i = x.size() - 1; i > -1; --1) {
            vec.erase(vec.begin() + x[i]);
        }
    };

    if constexpr (Soft) {

        if (!in_view) {
            in_view = true;
            row_view_idx.resize(old_nrow);
            std::iota(row_view_idx.begin(), row_view_idx.end(), 0);
            compact_block(row_view_idx);
        } else {
            compact_block(row_view_idx);
        }

    } else {

        if (in_view) {
            std::cerr << "Can't perform this operation while `in_view` mode activated, consider applying `.materialize()`\n";
            return;
        }
   
        auto process_container = [&compact_block,
                                  &x](auto& matr, const size_t idx_type) {

            const size_t ncols_cur = matr_idx[idx_type];

            if constexpr (CORES > 1) {

                int numa_nodes = 1;
                if (numa_available() >= 0) 
                    numa_nodes = numa_max_node() + 1;

                #pragma omp parallel if(CORES > 1) num_threads(CORES)
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
                        compact_block(matr[cpos]); 

                }

            } else {

                for (size_t cpos = 0; cpos < ncols_cur; ++cpos)
                    compact_block(matr[cpos]); 

            }

        };

        process_container(str_v,  0);
        process_container(chr_v,  1);
        process_container(bool_v, 2);
        process_container(int_v,  3);
        process_container(uint_v, 4);
        process_container(dbl_v,  5);

        if (!name_v_row.empty()) {
            auto& aux = name_v_row;
            for (int i = x.size() - 1; i > -1; --1)
                aux.erase(aux.begin() + i);
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




