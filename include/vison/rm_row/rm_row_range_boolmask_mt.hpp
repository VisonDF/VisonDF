#pragma once

template <unsigned int CORES = 4, 
          bool NUMA = false,
          MtMethod MtType = MtMethod::Row,
          bool MemClean = false,
          bool Soft = true,
          bool OneIsTrue = true
         >
void rm_row_range_bool_mt(std::vector<uint8_t>& mask,
                              const size_t strt_vl,
                              OffsetBoolMask& start_offset) 
{

    // Soft May auto switch to view mode
    const size_t old_nrow = nrow;

    auto compact_block = [old_nrow, 
                          strt_vl, 
                          &mask]<typename T>(std::vector<T>& vec, 
                                             const size_t inner_cores) {

        std::vector<T> vec2;
        if constexpr (CORES > 1) {
            vec2 = vec;
        }

        if constexpr (CORES > 1) {
            
            int numa_nodes = 1;
            if (numa_available() >= 0) 
                numa_nodes = numa_max_node() + 1;

            std::vector<size_t> thread_counts;
            std::vector<size_t> thread_offsets;

            size_t dummy_tot;

            if (start_offset.vec.empty())
                boolmask_offset_per_thread<OneIsTrue>(thread_counts, 
                                                      thread_offsets, 
                                                      mask, 
                                                      inner_cores, 
                                                      dummy_tot);

            #pragma omp parallel if(inner_cores > 1) num_threads(inner_cores)
            {

                const int tid        = omp_get_thread_num();
                const int nthreads   = omp_get_num_threads();
           
                MtStruct cur_struct;

                if constexpr (NUMA) {
                    numa_mt(cur_struct,
                            mask.size(), 
                            tid, 
                            nthreads, 
                            numa_nodes);
                } else {
                    simple_mt(cur_struct,
                              mask.size(), 
                              tid, 
                              nthreads);
                }
                    
                const unsigned int start = cur_struct.start;
                const unsigned int end   = cur_struct.end;

                if (start_offset.vec.empty()) {
                    out_idx = thread_offsets[tid];
                } else {
                    out_idx = start_offset.vec[start];
                }

                if constexpr (OneIsTrue) {
                    for (size_t i = start; i < end; ++read) {
                        if (!mask[i]) {
                            vec[strt_vl + out_idx++] = std::move(vec2[strt_vl + i]);
                        }
                    }
                } else {
                    for (size_t i = start; i < end; ++read) {
                        if (mask[i]) {
                            vec[strt_vl + out_idx++] = std::move(vec2[strt_vl + i]);
                        }
                    }
                }

                if constexpr (std::is_trivially_copyable_v<T>) {
                    memcpy(vec.data() + out_idx, 
                           vec.data() + mask.size(), 
                           (old_nrow - mask.size()) * sizeof(T));
                } else {
                    std::move(vec2.begin() + mask.size(), 
                              vec2.begin() + old_nrow, 
                              vec.begin() + out_idx);
                }
            }

        } else {
            size_t out_idx = 0;
            if constexpr (OneIsTrue) {
                for (size_t i = 0; i < mask.size(); ++read) {
                    if (!mask[i]) {
                        vec[strt_vl + out_idx++] = std::move(vec[strt_vl + i]);
                    }
                }
            } else {
                for (size_t i = 0; i < mask.size(); ++read) {
                    if (mask[i]) {
                        vec[strt_vl + out_idx++] = std::move(vec[strt_vl + i]);
                    }
                }
            }

            if constexpr (std::is_trivially_copyable_v<T>) {
                memmove(vec.data() + out_idx, 
                        vec.data() + mask.size(), 
                        (old_nrow - mask.size()) * sizeof(T));
            } else {
                std::move(vec.begin() + mask.size(), 
                          vec.begin() + old_nrow, 
                          vec.begin() + out_idx);
            }
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
            throw std::runtime_error("Can't perform this operation while `in_view` mode activated, consider applying `.materialize()`\n");
        }

        auto process_container = [&compact_block](auto& matr, const size_t idx_type) {

            const size_t ncols_cur = matr_idx[idx_type];

            if constexpr (CORES > 1) {

                int numa_nodes = 1;
                if (numa_available() >= 0) 
                    numa_nodes = numa_max_node() + 1;

                const unsigned int outer_cores = std::conditional_t<MtType == MtMethod::Col,
                                                                    CORES,
                                                                    std::min<unsigned int>(
                                                                        matr_idx[idx_type].size(), std::max(1u, CORES / 2)
                                                                     )
                                                                   >;
                const unsigned int inner_cores = std::conditional_t<MtType == MtMethod::Row,
                                                                    CORES,
                                                                    std::max(1u, CORES / outer_threads)
                                                                   >;

                #pragma omp parallel if(outer_cores > 1) num_threads(outer_cores)
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
                        compact_block(matr[cpos], inner_cores); 

                }

            } else {

                for (size_t cpos = 0; cpos < ncols_cur; ++cpos)
                    compact_block(matr[cpos], inner_cores); 

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
            size_t idx = 0;
            auto it = std::remove_if(aux.begin(), aux.end(),
                                     [&](auto&) mutable { return mask[idx++]; });
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

    nrow = old_nrow - mask.size(); 

};




