#pragma once

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool MapCol                  = false,
          bool IsDense                 = false, // assumed sorted
          bool IdxIsTrue               = true,
          AssertionType AssertionLevel = AssertionType::Normal
        >
void get_dataframe_filter_idx_mt(
                                 const std::vector<size_t>& cols, 
                                 Dataframe& cur_obj,
                                 const std::vector<unsigned int>& mask,
                                 Runs& runs = default_idx_runs
                                 )
{

    if constexpr (AssertionLevel == AssertionType::Hard) {
        if constexpr (!IdxIsTrue || IsDense) {
            unsigned int ref_val = mask[0];
            for (size_t i = 1; i < mask.size(); ++i) {
                if (ref_val < mask[i]) 
                    throw std::runtime_error("mask is not sorted increasingly\n");
                ref_val = mask[i];
            }
        } else {
            for (auto el : mask) {
                if (el >= nrow2)
                    throw std::runtime_error("mask index out of bouds\n");
            }
        }
    }

    if constexpr (AssertionLevel > AssertionType::None) {
        if constexpr (!IdxIsTrue) {
            if (mask.back() >= nrow2)
                throw std::runtime_error("mask indices are exceeding nrow\n");
        }
    }

    auto find_col_base = [this]([[maybe_unused]] const auto &idx_vec, 
                                [[maybe_unused]] const size_t idx_type,
                                const size_t x) -> size_t 
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

    const size_t nrow2 = cur_obj.get_nrow();
    const size_t n_el = (Periodic) ? nrow2 : mask.size() ;
    const size_t n_el2 = mask.size();

    std::vector<size_t> thread_counts;
    if constexpr (CORES > 1) {
        if constexpr (IsDense) {
            if (runs.vec.empty() || runs.thread_offsets.empty()) {
                build_runs_mt<IdxIsTrue,
                              Periodic>(
                                        runs.thread_offsets,
                                        thread_counts,
                                        mask,
                                        CORES,
                                        runs,
                                        runs.active_rows,
                                        n_el,
                                        n_el2
                                       );
            }
        } else {
            if (runs.thread_offsets.empty()) {
                build_runs_mt_simple<IdxIsTrue,
                                     Periodic>(
                                               runs.thread_offsets,
                                               mask,
                                               CORES,
                                               runs.active_rows,
                                               n_el,
                                               n_el2
                                              );
            }
        }
    } else {
        if constexpr (IsDense) {
            if (runs.empty()) {
                if (runs.thread_offsets.empty()) {
                    build_runs<IdxIsTrue,
                               Periodic>(
                                         mask,
                                         runs.vec,
                                         runs.active_rows,
                                         n_el,
                                         n_el2
                                        );
                }
            }
        } else {
            if (runs.thread_offsets.empty()) {
                build_runs_simple<IdxIsTrue,
                                  Periodic>(
                                            mask,
                                            active_rows,
                                            n_el,
                                            n_el2
                                           );
            }
        }
    }

    const unsigned int local_nrow = runs.active_rows;
    nrow = local_nrow;

    in_view = cur_obj.get_in_view();
    ankerl::unordered_dense::set<unsigned int>& col_alrd_materialized2  = cur_obj.get_col_alrd_materialized();
    const auto row_view_idx2                                            = cur_obj.get_row_view_idx();
    const std::vector<std::string>& name_v_row2                         = cur_obj.get_rowname();

    if (in_view) {
        row_view_idx.resize(local_nrow);
    }

    auto copy_col_dense = [&mask,
                           &runs,
                           &thread_counts,
                           n_el,
                           n_el2]<typename T>(
                                              std::vector<T>& dst_vec,
                                              const std::vector<T>& src_vec2
                                             )
    {
        const std::string* __restrict src = src_vec2.data();
        std::string*       __restrict dst = dst_vec.data();

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

                const size_t delta  = runs.thread_counts[tid];
                const size_t delta2 = (!IdxIsTrue) ? runs.thread_offsets[tid] : 0; // else dummy val

                copy_col_filter_idx_dense<IdxIsTrue,
                                          Periodic,
                                          true      // copy (distinct)
                                         >(
                                            dst,
                                            src,
                                            runs,
                                            delta,
                                            delta2,
                                            cur_start,
                                            cur_end
                                          );
            }

        } else {

            const size_t delta  = 0;  // dummy val
            const size_t delta2 = 0;  // dummy val

            copy_col_filter_idx_dense<IdxIsTrue,
                                         Periodic,
                                         true      // copy (distinct)
                                        >(
                                           dst,
                                           src,
                                           runs,
                                           delta,
                                           delta2,
                                           0,
                                           n_el
                                         );

        }
    };

    auto copy_col = [&mask,
                     n_el,
                     n_el2](
                            auto& dst_vec,
                            const auto& src_vec2
                           )
    {
        const std::string* __restrict src = src_vec2.data();
        std::string*       __restrict dst = dst_vec.data();
   
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
                    
                const unsigned int start = cur_struct.start;
                const unsigned int end   = cur_struct.end;

                size_t out_idx = (!IdxIsTrue) ? runs.thread_offsets[tid] : 0; // else dummy_val

                copy_col_filter_idx<IdxIsTrue,
                                    Periodic,
                                    true      // copy
                                   >(
                                     dst,
                                     src,
                                     mask,
                                     out_idx,
                                     cur_start,
                                     cur_end,
                                     n_el2
                                    );

            }

        } else {

            size_t out_idx = 0; // dummy val

            copy_col_filter_idx<IdxIsTrue,
                                Periodic,
                                true      // copy
                               >(
                                  dst,
                                  src,
                                  mask,
                                  out_idx,
                                  0,
                                  n_el,
                                  n_el2
                                 );
        }
    };

    const auto& str_vec2  = cur_obj.get_str_vec();
    const auto& chr_vec2  = cur_obj.get_chr_vec();
    const auto& bool_vec2 = cur_obj.get_bool_vec();
    const auto& int_vec2  = cur_obj.get_int_vec();
    const auto& uint_vec2 = cur_obj.get_uint_vec();
    const auto& dbl_vec2  = cur_obj.get_dbl_vec();

    auto process_container = [local_nrow]<typename T>(const std::vector<std::vector<T>>& matr2,
                                                      std::vector<std::vector<T>>& matr){
        for (const auto& el : matr2) {
            matr.emplace_back();
            matr.back().resize(local_nrow);
            auto* dst       = matr_v.back().data();
            const auto* src = el.data();
            if constexpr (!IsDense || std::is_same_v<T, std::string>) {
                copy_col(dst, src);
            } else {
                copy_col_dense(dst, src);
            }
        }
    };

    auto cols_proceed = [local_nrow, 
                         &find_col_base,
                         &col_alrd_materialized2,
                         &cols](auto&& f1, auto&& f2) 
    {
        size_t i2 = 0;
        for (int i : cols) {

            switch (type_refv[i]) {
                  case 's': {
                              matr_idx_map[0] = i2;
                              str_v.emplace_back();
                              str_v.back().resize(local_nrow);
                              const size_t idx_in_type = find_col_base(matr_idx[0], 0, i);
                              f2(str_v.back(),  
                                 str_vec2[idx_in_type]); 
                              break;
                            }
                  case 'c': {
                              matr_idx_map[1] = i2;
                              chr_v.emplace_back();
                              chr_v.back().resize(local_nrow);
                              const size_t idx_in_type = find_col_base(matr_idx[1], 1, i);
                              f1(chr_v.back(),  
                                 chr_vec2[idx_in_type]); 
                              break;
                            }
                  case 'b': {
                              matr_idx_map[2] = i2;
                              bool_v.emplace_back();
                              bool_v.back().resize(local_nrow);
                              const size_t idx_in_type = find_col_base(matr_idx[2], 2, i);
                              f1(bool_v.back(),  
                                 bool_vec2[idx_in_type]); 
                              break;
                            }
                  case 'i': {
                              matr_idx_map[3] = i2;
                              int_v.emplace_back();
                              int_v.back().resize(local_nrow);
                              const size_t idx_in_type = find_col_base(matr_idx[3], 3, i);
                              f1(int_v.back(),  
                                 int_vec2[idx_in_type]); 
                              break;
                            }
                 case 'u': {
                              matr_idx_map[4] = i2;
                              uint_v.emplace_back();
                              uint_v.back().resize(local_nrow);
                              const size_t idx_in_type = find_col_base(matr_idx[4], 4, i);
                              f1(uint_v.back(),  
                                 uint_vec2[idx_in_type]); 
                              break;
                            }
                  case 'd': {
                              matr_idx_map[5] = i2;
                              dbl_v.emplace_back();
                              dbl_v.back().resize(local_nrow);
                              const size_t idx_in_type = find_col_base(matr_idx[5], 5, i);
                              f1(dbl_v.back(),  
                                 dbl_vec2[idx_in_type]); 
                              break;
                            }
            }

            if (col_alrd_materialized2.contains(i))
                col_alrd_materialized.insert(i);
                
            name_v[i2]    = name_v1[i];
            type_refv[i2] = type_refv1[i];

            i2 += 1;

        }

    };

    if (cols.empty()) {

        col_ard_materialized = col_alrd_materialized2;
        matr_idx     = cur_obj.get_matr_idx();
        matr_idx_map = cur_obj.get_matr_idx_map();
        sync_map_col = cur_obj.get_sync_map_col();
        ncol         = cur_obj.get_ncol();

        process_container(str_v2,  str_v);
        process_container(chr_v2,  chr_v);
        process_container(bool_v2, bool_v);
        process_container(int_v2,  int_v);
        process_container(uint_v2, uint_v);
        process_container(dbl_v2,  dbl_v);

        name_v    = cur_obj.get_colname();
        type_refv = cur_obj.get_typecol(); 

    } else {

        ncol = cols.size();

        if (in_view)
            col_ard_materialized.reserve(ncol);

        const auto& name_v1    = cur_obj.get_colname();
        const auto& type_refv1 = cur_obj.get_typecol();

        type_refv.resize(ncol);
        name_v.resize(ncol);

        sync_map_col = {true, true, true, true, true, true};

        if constexpr (!IsDense) {
            cols_proceed(copy_col, copy_col);
        } else {
            cols_proceed(copy_col_dense, copy_col);
        }

    }

    if (!name_v_row2.empty())
        copy_col(name_v_row, name_v_row2);

    if constexpr (!IsDense) {
        copy_col(row_view_idx, row_view_idx2);
    } else {
        copy_view_dense(row_view_idx, row_view_idx2);
    }

}



