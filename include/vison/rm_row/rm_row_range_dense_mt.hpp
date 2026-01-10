#pragma once

template <unsigned int CORES = 4,
          bool NUMA = false,
          bool MemClean = false,
          bool Soft = true,
          bool Sorted = false,  // if not, it will modify x
          bool SanityCheck = true,
         >
void rm_row_range_inplace_mt(std::vector<unsigned int>& x)
{

    const size_t old_nrow = nrow;
    if (x.empty() || old_nrow == 0) return;
    if constexpr (!Sorted) std::sort(x.begin(), x.end());

    const size_t new_nrow = old_nrow - x.size();

    auto compact_block_pod = [old_nrow, &x]<typename T>(std::vector<T>& dst, 
                                                        std::vector<T>& src) 
    {

        size_t i  = 0;
        size_t i2 = 0;
        size_t written = x[0];
        while (i2 < x.size()) {
        
            unsigned ref_val = x[i2++];
            size_t start = i;
            while (i < ref_val) ++i;
        
            size_t len = i - start;
            {
                T* __restrict d = dst.data() + written;
                T* __restrict s = src.data() + start;
       
                memmove(d, s, len * sizeof(T))
            }
        
            written += len;
            i += 1;
        }
        while (i < old_nrow) {
            dst[written] = src[i];
            i += 1;
            written += 1;
        };
    };


    if constexpr (Soft) {

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

        if (!in_view) {
            in_view = true;
            row_view_idx.resize(new_nrow);
            std::iota(row_view_idx.begin(), row_view_idx.end(), 0);
        }

        compact_block_pod(row_view_idx, row_view_idx);

    } else {

        if (in_view) {
            std::cerr << "Can't perform this operation while `in_view` mode activated, consider applying `.materialize()`\n";
            return;
        }

        auto compact_block_scalar = [old_nrow, &x](auto& dst, 
                                                   auto& src) 
        {
            size_t i  = 0;
            size_t i2 = 0;
            size_t written = x[0];
            while (i2 < x.size()) {
                const unsigned int ref_val = x[i2++];
                while (i < ref_val) {
                    dst[written] = std::move(src[i]);
                    i += 1;
                    written += 1;
                };
                i += 1;
            }
            while (i < old_nrow) {
                dst[written] = std::move(src[i]);
                i += 1;
                written += 1;
            };
        };

        auto process_container = [&x](auto&& f,
                                      auto& matr, 
                                      const size_t idx_type) 
        {

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
                        f(matr[cpos]); 

                }

            } else {

                for (size_t cpos = 0; cpos < ncols_cur; ++cpos)
                    f(matr[cpos]); 

            }

        };

        process_container(compact_block_scalar, str_v,  0);
        process_container(compact_block_pod,    chr_v,  1);
        process_container(compact_block_pod,    bool_v, 2);
        process_container(compact_block_pod,    int_v,  3);
        process_container(compact_block_pod,    uint_v, 4);
        process_container(compact_block_pod,    dbl_v,  5);

        if (!name_v_row.empty()) {
            compact_block_scalar(name_v_row, name_v_row);
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



