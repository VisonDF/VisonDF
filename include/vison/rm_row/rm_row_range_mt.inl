#pragma once

template <unsigned int CORES           = 4, 
          bool NUMA                    = false,
          MtMethod MtType              = MtMethod::Row,
          bool MemClean                = false,
          bool Soft                    = true,
          bool Sorted                  = true,
          bool IdxIsTrue               = true,
          AssertionType AssertionLevel = AssertionType::Simple
         >
void rm_row_range_mt(std::vector<uint8_t>& mask,
                     Runs& runs = Runs{}) 
{

    // Soft May auto switch to view mode
    const size_t old_nrow = nrow;

    const size_t n_el = (IdxIsTrue) ? n_el : old_nrow - n_el;


    if constexpr (IsDense && !Sorted) {
        throw std::runtime_error("To use `IsDense` parameter, you must sort the mask\n");
    }

    if constexpr (!IsSorted && !IdxIsTrue) {
        std::sort(mask.begin(), mask.end());
    }

    if constexpr (!IdxIsTrue) {
        if (mask.back() >= old_nrow)
            throw std::runtime_error("mask indices are exceeding nrow\n");
    }

    if constexpr (AssertionLevel == AssertionType::Hard) {
        if constexpr (IdxIsTrue && IsDense) {
            const ref_val = mask[0];
            for (size_t i = 1; i < mask.size(); ++i) {
                if (ref_val < mask[i]) [[unlikely]] {
                    throw std::runtime_error("mask is not sorted ascendingly\n");
                }
            }
            if (mask.back() >= old_nrow) {
                throw std::runtime_error("mask indices are out of bound\n");
            }
        }
    }

    auto compact_block = [old_nrow, 
                          n_el,
                          &runs,
                          &mask]<typename T>(std::vector<T>& vec, 
                                             const size_t inner_cores) {

        T* src;
        std::vector<T> vec2;
        if constexpr (CORES > 1 || !Sorted && IdxIsTrue) {
            vec2 = vec;
            src = &vec2;
        } else {
            src = &vec;
        }

        if constexpr (CORES > 1) {
           
            if constexpr (!IdxIsTrue) {
                if (runs.thread_offsets.empty()) { 
                    idx_offset_per_thread_mt_simple<IdxIsTrue>(runs.thread_offsets,
                                                               mask,
                                                               CORES);
                }
            }

            int numa_nodes = 1;
            if (numa_available() >= 0) 
                numa_nodes = numa_max_node() + 1;

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

                if constexpr (IdxIsTrue) {
                    for (size_t i = start; i < end; ++i)
                        vec[i] = std::move((*src)[mask[i]]);
                } else {
                    size_t out_idx = runs.thread_offsets[tid];
                    for (size_t i = start; i < end; ++i) {
                        const size_t next_stop = mask[i];
                        while (out_idx < next_stop) {
                            vec[out_idx] = std::move((*src)[out_idx]);
                            out_idx += 1;
                        }
                    }
                }

            }

        } else {

            if constexpr (IdxIsTrue) {
                for (size_t i = 0; i < mask.size(); ++i)
                    vec[i] = std::move((*src)[mask[i]]);
            } else {
                size_t out_idx = 0;
                for (size_t i = 0; i < mask.size(); ++i) {
                    const size_t next_stop = mask[i];
                    while (out_idx < next_stop) {
                        vec[out_idx] = std::move((*src)[out_idx]);
                        out_idx += 1;
                    }
                    out_idx += 1;
                }
            }
        }

        if constexpr (MemClean) {
            vec.resize(n_el);
            vec.shrink_to_fit();
        }

    };

    if constexpr (Soft) {

        if (!in_view) {
            in_view = true;
            row_view_idx.resize(old_nrow);
            std::iota(row_view_idx.begin(), row_view_idx.end(), 0);
            compact_block(row_view_idx, CORES);
        } else {
            compact_block(row_view_idx, CORES);
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
                    compact_block(matr[cpos], CORES); 

            }

        };

        process_container(str_v,  0);
        process_container(chr_v,  1);
        process_container(bool_v, 2);
        process_container(int_v,  3);
        process_container(uint_v, 4);
        process_container(dbl_v,  5);

        if (!name_v_row.empty()) {
            compact_block(name_v_row, CORES);
        }

    }

    nrow = (IdxIsTrue) ? old_nrow - mask.size() : mask.size(); 

};




