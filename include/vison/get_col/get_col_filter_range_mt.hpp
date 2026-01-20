#pragma once

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool IsBool                  = false,
          bool MapCol                  = false,
          bool IsDense                 = false,
          bool OneIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename T
        >
void get_col_filter_range(
                          unsigned int x,
                          std::vector<T> &rtn_v,
                          const std::vector<uint8_t> &mask,
                          const unsigned int strt_vl,
                          OffsetBoolMask& offset_start = default_offset_start
                         )
{

    const unsigned int n_el  = (!Periodic) ? mask.size() : nrow - strt_vl;
    const unsigned int n_el2 = mask.size();

    if constexpr (AssertionLevel > AssertionType::None) {
        if (start_vl + mask.size >= nrow) {
            throw std::runtime_error("strt_vl + mask.size() > nrow\n");
        }
        if constexpr (IsDense && std::is_same_v<T, std::string>) {
            throw std::runtime_error("IsDense && std::is_same_v<T, std::string>\n");
        }
    }

    auto find_col_base = [this,
                          x]([[maybe_unused]] const auto &idx_vec, 
                             [[maybe_unused]] const size_t idx_type) -> size_t 
    {
        size_t pos;

        if constexpr (!MapCol) {
            pos = 0;
            while (pos < idx_vec.size() && idx_vec[pos] != x)
                ++pos;

            if (pos == idx_vec.size()) {
                throw std::runtime_error("Error in (get_col), no column found\n");
            }
        } else {
            if (!matr_idx_map[idx_type].contains(x)) {
                throw std::runtime_error("MapCol chosen but col not found in map\n");
            }
            if (!sync_map_col[idx_type]) {
                throw std::runtime_error("Map not synced\n");
            }
            pos = matr_idx_map[idx_type][x];
        }
        return pos;
    };

    auto extract_masked = [&rtn_v, 
                           &mask, 
                           &offset_start,
                           n_el2,
                           n_el](const auto*__restrict src) {

        if (offset_start.vec.empty()) {
            build_boolmask<OneIsTrue,
                           Periodic>(offset_start.thread_offsets, 
                                     mask, 
                                     CORES,
                                     offset_start.active_rows,
                                     n_el2);
        }
        rtn_v.resize(offset_start.active_rows);

        if constexpr (CORES > 1) {

            if (CORES > n_el)
                throw std::runtime_error("Too much cores for so little nrows\n");

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
                            n_el, 
                            tid, 
                            nthreads, 
                            numa_nodes);
                } else {
                    simple_mt(cur_struct,
                              n_el, 
                              tid, 
                              nthreads);
                }
                    
                const unsigned int cur_start = cur_struct.start;
                const unsigned int cur_end   = cur_struct.end;
           
                const size_t out_idx = offset_start.thread_offsets[tid];

                copy_col_filter_range<OneIsTrue,
                                      Periodic,
                                      true     // distinct
                                      >(
                                        rtn_v.data(),
                                        src,
                                        mask,
                                        out_idx,
                                        cur_start,
                                        cur_end,
                                        n_el2
                                       );
            }

        } else {

            size_t out_idx = 0;  // dummy_val

            copy_col_filter_range<OneIsTrue,
                                  Periodic,
                                  true      // distinct
                                 >(
                                   rtn_v.data(),
                                   src,
                                   mask,
                                   out_idx,
                                   0,
                                   n_el,
                                   n_el2
                                  );
        }

    };

    auto extract_masked_dense = [&rtn_v, 
                                 &mask, 
                                 &offset_start,
                                 n_el2,
                                 n_el]<typename TB>(const TB* __restrict src) {

        if (offset_start.vec.empty()) {
            build_boolmask<OneIsTrue,
                           Periodic>(offset_start.thread_offsets, 
                                     mask, 
                                     CORES,
                                     ofset_start.active_rows,
                                     n_el,
                                     n_el2);
        }
        rtn_v.resize(offset_start.active_rows);

        if constexpr (CORES > 1) {

            if (CORES > n_el)
                throw std::runtime_error("Too much cores for so little nrows\n");

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
                            n_el, 
                            tid, 
                            nthreads, 
                            numa_nodes);
                } else {
                    simple_mt(cur_struct,
                              n_el, 
                              tid, 
                              nthreads);
                }
                    
                const unsigned int cur_start = cur_struct.start;
                const unsigned int cur_end   = cur_struct.end;
            
                const size_t out_idx = offset_start.thread_offsets[tid];

                size_t i = cur_start;

                copy_col_filter_range_dense<
                                            OneIsTrue,
                                            Periodic,
                                            true     // distinct
                                           >(
                                              rtn_v.data(),
                                              src,
                                              mask,
                                              i,
                                              out_idx,
                                              cur_start,
                                              cur_end,
                                              n_el2
                                            );
            }

        } else {

            size_t out_idx = 0;
            size_t i       = 0;

            copy_col_filter_range_dense<
                                        OneIsTrue,
                                        Periodic,
                                        true     // distinct
                                       >(
                                          rtn_v.data(),
                                          src,
                                          mask,
                                          i,
                                          out_idx,
                                          0,
                                          n_el,
                                          n_el2
                                        );
        }
    }

    if constexpr (std::is_same_v<T, std::string>) {

        const size_t pos_base = find_col_base(matr_idx[0], 0);
        extract_masked(str_v[pos_base].data() + strt_vl);

    } else if constexpr (std::is_same_v<T, CharT>) {

        const size_t pos_base = find_col_base(matr_idx[1], 1);
        if constexpr (!IsDense) {

            extract_masked(chr_v[pos_base].data() + strt_vl);

        } else {

            extract_masked_dense(chr_v[pos_base].data() + strt_vl);

        }

    } else if constexpr (IsBool) {

        const size_t pos_base = find_col_base(matr_idx[2], 2);

        if constexpr (!IsDense) {

            extract_masked(bool_v[pos_base].data() + strt_vl);

        } else {

            extract_masked_dense(bool_v[pos_base].data() + strt_vl);

        }

    } else if constexpr (std::is_same_v<T, IntT>) {

        const size_t pos_base = find_col_base(matr_idx[3], 3);

        if constexpr (!IsDense) {

            extract_masked(int_v[pos_base].data() + strt_vl);

        } else {

            extract_masked_dense(int_v[pos_base].data() + strt_vl);

        }

    } else if constexpr (std::is_same_v<T, UIntT>) {

        const size_t pos_base = find_col_base(matr_idx[4], 4);

        if constexpr (!IsDense) {

            extract_masked(uint_v[pos_base].data() + strt_vl);

        } else {

            extract_masked_dense(uint_v[pos_base].data() + strt_vl);

        }

    } else if constexpr (std::is_same_v<T, FloatT>) {

        const size_t pos_base = find_col_base(matr_idx[5], 5);

        if constexpr (!IsDense) {

            extract_masked(dbl_v[pos_base].data() + strt_vl);

        } else {

            extract_masked_dense(dbl_v[pos_base].data() + strt_vl);

        }

    } else {
        throw std::runtime_error("Error in (get_col), unsupported type\n");
    }

}



