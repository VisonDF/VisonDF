#pragma once

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool MapCol                  = false,
          bool IsDense                 = false,
          bool OneIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple
    >
void get_dataframe_filter_range_mt(
                                   const std::vector<size_t>& cols, 
                                   Dataframe& cur_obj,
                                   const std::vector<uint8_t>& mask,
                                   const size_t strt_vl,
                                   OffsetBoolMask& offset_start = default_offset_start
                                   )
{

    if constexpr (AssertionLevel > AssertionType::Simple) {
        if (strt_vl + mask.size() > cur_obj.get_nrow()) {
            throw std::runtime_error("strt_vl + mask.size() > nrow\n");
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

    const unsigned int n_el  = (!Periodic) ? mask.size() : nrow - strt_vl;
    const unsigned int n_el2 = mask.size();

    if (offset_start.thread_offsets.empty()) {
        build_boolmask<OneIsTrue,
                       Periodic>(offset_start.thread_offsets, 
                                 mask, 
                                 CORES,
                                 offset_start.active_rows,
                                 n_el,
                                 n_el2);
    }

    nrow = offset_start.active_rows;

    in_view = cur_obj.get_in_view();
    ankerl::unordered_dense::set<unsigned int>& col_alrd_materialized2  = cur_obj.get_col_alrd_materialized();
    const auto row_view_idx2                                            = cur_obj.get_row_view_idx();    
    const std::vector<std::string>& name_v_row2                         = cur_obj.get_rowname();

    auto copy_col_dns = [strt_vl,
                         n_el,
                         n_el2,
                         &mask, 
                         &offset_start]<typename T>(
                                                     std::vector<T>& dst_vec,
                                                     const std::vector<T>& src_vec2
                                                   )
    {

        dst_vec.resize(offset_start.active_rows);

        const T* __restrict src = src_vec2.data() + strt_vl;
        T*       __restrict dst = dst_vec.data();

        if constexpr (CORES > 1) {

            if (CORES > mask.size())
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
                                              dst,
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
                                          dst,
                                          src,
                                          mask,
                                          i,
                                          out_idx,
                                          0,
                                          n_el,
                                          n_el2
                                        );
        }
    };

    auto copy_col = [strt_vl,
                     n_el,
                     n_el2,
                     &mask, 
                     &offset_start]<typename T>(
                                                auto& dst_vec,
                                                const auto& src_vec2
                                               )
    {
  
        dst_vec.resize(offset_start.active_rows);

        const T* __restrict src = src_vec2.data() + strt_vl;
        T*       __restrict dst = dst_vec.data();

        if constexpr (CORES > 1) {

            if (CORES > mask.size())
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
                                      true      // distinct
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

            size_t out_idx = 0;

            copy_col_filter_range<OneIsTrue,
                                  Periodic,
                                  true       // distinct
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

    auto process_container = []<typename T>(const std::vector<std::vector<T>>& matr2,
                                            std::vector<std::vector<T>>& matr){
        for (const auto& el : matr2) {
            matr.emplace_back();
            auto* dst       = matr_v.back().data();
            const auto* src = el.data();
            if constexpr (!IsDense || std::is_same_v<T, std::string>) {
                copy_col(dst, src);
            } else {
                copy_col_dns(dst, src);
            }
        }
    };

    auto cols_proceed = [&find_col_base,
                         &col_alrd_materialized2,
                         &cols](auto&& f1, auto&& f2) 
    {
        size_t i2 = 0;
        for (int i : cols) {

            switch (type_refv[i]) {
                  case 's': {
                              matr_idx_map[0] = i2;
                              str_v.emplace_back();
                              const size_t idx_in_type = find_col_base(matr_idx[0], 0, i);
                              f2(str_v.back(),  
                                 str_vec2[idx_in_type]); 
                              break;
                            }
                  case 'c': {
                              matr_idx_map[1] = i2;
                              chr_v.emplace_back();
                              const size_t idx_in_type = find_col_base(matr_idx[1], 1, i);
                              f1(chr_v.back(),  
                                 chr_vec2[idx_in_type]); 
                              break;
                            }
                  case 'b': {
                              matr_idx_map[2] = i2;
                              bool_v.emplace_back();
                              const size_t idx_in_type = find_col_base(matr_idx[2], 2, i);
                              f1(bool_v.back(),  
                                 bool_vec2[idx_in_type]); 
                              break;
                            }
                  case 'i': {
                              matr_idx_map[3] = i2;
                              int_v.emplace_back();
                              const size_t idx_in_type = find_col_base(matr_idx[3], 3, i);
                              f1(int_v.back(),  
                                 int_vec2[idx_in_type]); 
                              break;
                            }
                 case 'u': {
                              matr_idx_map[4] = i2;
                              uint_v.emplace_back();
                              const size_t idx_in_type = find_col_base(matr_idx[4], 4, i);
                              f1(uint_v.back(),  
                                 uint_vec2[idx_in_type]); 
                              break;
                            }
                  case 'd': {
                              matr_idx_map[5] = i2;
                              dbl_v.emplace_back();
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
        name_v    = cur_obj.get_colname();
        type_refv = cur_obj.get_typecol(); 

        process_container(str_v2,  str_v);
        process_container(chr_v2,  chr_v);
        process_container(bool_v2, bool_v);
        process_container(int_v2,  int_v);
        process_container(uint_v2, uint_v);
        process_container(dbl_v2,  dbl_v);

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
            cols_proceed(copy_col_dns, copy_col);
        }
    }

    if (!name_v_row2.empty())
        copy_col(name_v_row, name_v_row2);

    if constexpr (!IsDense) {
        if (in_view)
            copy_col(row_view_idx, row_view_idx2);
    } else {
        if (in_view)
            copy_col_dense(row_view_idx, row_view_idx2);
    }

}







