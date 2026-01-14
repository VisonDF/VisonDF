#pragma once

template <unsigned int CORES = 4,
          bool NUMA = false,
          bool IsDense = false,
          bool OneIsTrue = true,
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

    if (offset_start.thread_offsets.empty()) {
        build_boolmask<OneIsTrue>(offset_start.thread_offsets, 
                                  mask, 
                                  CORES,
                                  offset_start.active_rows);
    }

    nrow = offset_start.active_rows;

    in_view = cur_obj.get_in_view();
    ankerl::unordered_dense::set<unsigned int>& col_alrd_materialized2  = cur_obj.get_col_alrd_materialized();
    const auto row_view_idx2                                            = cur_obj.get_row_view_idx();    
    const std::vector<std::string>& name_v_row2                         = cur_obj.get_rowname();

    auto copy_col_dense = [&mask, 
                           &offset_start]<typename T>(
                                                       std::vector<T>& dst_vec,
                                                       const std::vector<T>& src_vec2
                                                     )
    {

        dst_vec.resize(offset_start.active_rows);

        const std::string* __restrict src = src_vec2.data();
        std::string*       __restrict dst = dst_vec.data();

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
                    
                const unsigned int cur_start = cur_struct.start;
                const unsigned int cur_end   = cur_struct.end;

                const size_t out_idx = offset_start.thread_offsets[tid];

                size_t i = cur_start;
                if constexpr (OneIsTrue) {
                    while (!mask[i]) {
                        i += 1;
                        out_idx += 1;
                    }
                } else {
                    while (mask[i]) {
                        i += 1;
                        out_idx += 1;
                    }
                }
                while (i < cur_end) {
      
                    if constexpr (OneIsTrue) {
                        while (i < cur_end && mask[i]) {
                            i += 1;
                        }
                    } else {
                        while (i < cur_end && !mask[i]) {
                            i += 1;
                        }
                    }

                    const size_t start = i;
                    if constexpr (OneIsTrue) {
                        while (i < cur_end && !mask[i]) ++i;
                    } else {
                        while (i < cur_end && mask[i]) ++i;
                    }
                
                    size_t len = i - start;
                    {
                        T* __restrict d = dst.data() + out_idx;
                        T* __restrict s = src.data() + strt_vl + start;
       
                        memcpy(d, s, len * sizeof(T))

                    }
                
                    out_idx += len;
                    i += 1;
                }

            }

        } else {

            size_t out_idx = 0;
            size_t i       = 0;
            if constexpr (OneIsTrue) {
                while (!mask[i]) {
                    i += 1;
                    out_idx += 1;
                }
            } else {
                while (mask[i]) {
                    i += 1;
                    out_idx += 1;
                }
            }
            while (i < mask.size()) {
      
                if constexpr (OneIsTrue) {
                    while (i < mask.size() && mask[i]) {
                        i += 1;
                    }
                } else {
                    while (i < mask.size() && !mask[i]) {
                        i += 1;
                    }
                }

                const size_t start = i;
                if constexpr (OneIsTrue) {
                    while (i < mask.size() && !mask[i]) ++i;
                } else {
                    while (i < mask.size() && mask[i]) ++i;
                }
            
                size_t len = i - start;
                {
                    T* __restrict d = dst.data() + out_idx;
                    T* __restrict s = src.data() + strt_vl + start;
       
                    memcpy(d, s, len * sizeof(T));

                }
            
                out_idx += len;
                i += 1;
            }

        }
    };

    auto copy_view_dense = [&mask, 
                            &row_view_idx2, 
                            &offset_start]()
    {

        const std::string* __restrict src = src_vec2.data();
        std::string*       __restrict dst = dst_vec.data();

        if constexpr (CORES > 1) {

            if (CORES > mask.size())
                throw std::runtime_error("Too much cores for so little nrows\n");

            row_view_idx.resize(offset_start.active_rows);

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
                            mask.size(), 
                            tid, 
                            nthreads, 
                            numa_nodes);
                } else {
                    simple_mt(cur_struct,
                              mask..size(), 
                              tid, 
                              nthreads);
                }
                    
                const unsigned int start = cur_struct.start;
                const unsigned int len   = cur_struct.len;

                const size_t out_idx = offset_start.thread_offsets[tid];

                size_t i = cur_start;
                if constexpr (OneIsTrue) {
                    while (!mask[i]) {
                        i += 1;
                        out_idx += 1;
                    }
                } else {
                    while (mask[i]) {
                        i += 1;
                        out_idx += 1;
                    }
                }
                while (i < cur_end) {
      
                    if constexpr (OneIsTrue) {
                        while (i < cur_end && mask[i]) {
                            i += 1;
                        }
                    } else {
                        while (i < cur_end && !mask[i]) {
                            i += 1;
                        }
                    }

                    const size_t start = i;
                    if constexpr (OneIsTrue) {
                        while (i < cur_end && !mask[i]) ++i;
                    } else {
                        while (i < cur_end && mask[i]) ++i;
                    }
                
                    size_t len = i - start;
                    {

                        unsigned int* __restrict d = row_view_idx.data()  + out_idx;
                        unsigned int* __restrict s = row_view_idx2.data() + strt_vl + start;
       
                        memcpy(d, s, len * sizeof(unsigned int));

                    }
                
                    out_idx += len;
                    i += 1;
                }

            }

        } else {

            size_t out_idx = 0;
            size_t i       = 0;
            if constexpr (OneIsTrue) {
                while (!mask[i]) {
                    i += 1;
                    out_idx += 1;
                }
            } else {
                while (mask[i]) {
                    i += 1;
                    out_idx += 1;
                }
            }
            while (i < mask.size()) {
      
                if constexpr (OneIsTrue) {
                    while (i < mask.size() && mask[i]) {
                        i += 1;
                    }
                } else {
                    while (i < mask.size() && !mask[i]) {
                        i += 1;
                    }
                }

                const size_t start = i;
                if constexpr (OneIsTrue) {
                    while (i < mask.size() && !mask[i]) ++i;
                } else {
                    while (i < mask.size() && mask[i]) ++i;
                }
            
                size_t len = i - start;
                {

                    unsigned int* __restrict d = row_view_idx.data()  + out_idx;
                    unsigned int* __restrict s = row_view_idx2.data() + strt_vl + start;
       
                    memcpy(d, s, len * sizeof(unsigned int));

                }
            
                out_idx += len;
                i += 1;
            }

        }

    };

    auto copy_col = [&mask, 
                     &offset_start](
                                     auto& dst_vec,
                                     const auto& src_vec2
                                    )
    {
  
        dst_vec.resize(offset_start.active_rows);

        if constexpr (CORES > 1) {

            if (CORES > mask.size())
                throw std::runtime_error("Too much cores for so little nrows\n");

            const std::string* __restrict src = src_vec2.data();
            std::string*       __restrict dst = dst_vec.data();

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

                const size_t out_idx = offset_start.thread_offsets[tid];

                for (size_t j = start; j < end; ++j) {
                    if (!mask[j]) continue;
                    dst[out_idx++] = src[strt_vl + j];
                }

            }

        } else {

            const std::string* __restrict src = src_vec2.data();
            std::string*       __restrict dst = dst_vec.data();

            for (size_t j = 0; j < mask.size(); ++j) {
                if (!mask[j]) continue;
                dst[j] = src[strt_vl + j];
            }

        }
    };

    auto copy_view = [&mask,
                     &row_view_idx2,
                     &offset_start]()
    {
  
        dst_vec.resize(offset_start.active_rows);

        if constexpr (CORES > 1) {

            if (CORES > mask.size())
                throw std::runtime_error("Too much cores for so little nrows\n");

            const std::string* __restrict src = row_view_idx2.data();
            std::string*       __restrict dst = row_view_idx.data();

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

                const size_t out_idx = offset_start.thread_offsets[tid];

                for (size_t j = start; j < end; ++j) {
                    if (!mask[j]) continue;
                    dst[out_idx++] = src[strt_vl + j];
                }

            }

        } else {

            const std::string* __restrict src = src_vec2.data();
            std::string*       __restrict dst = dst_vec.data();

            for (size_t j = 0; j < mask.size(); ++j) {
                if (!mask[j]) continue;
                dst[j] = src[strt_vl + j];
            }

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
                copy_col_dense(dst, src);
            }
        }
    };

    auto cols_proceed = [&col_alrd_materialized2,
                         &cols](auto&& f1, auto&& f2) 
    {
        size_t i2 = 0;
        for (int i : cols) {

            switch (i) {
                  case 0: {
                              matr_idx_map[0] = i2;
                              str_v.emplace_back();
                              f2(str_v.back(),  
                                 str_vec2[i]); 
                              break;
                            }
                  case 1: {
                              matr_idx_map[1] = i2;
                              chr_v.emplace_back();
                              f1(chr_v.back(),  
                                 chr_vec2[i]); 
                              break;
                            }
                  case 2: {
                              matr_idx_map[2] = i2;
                              bool_v.emplace_back();
                              f1(bool_v.back(),  
                                 bool_vec2[i]); 
                              break;
                            }
                  case 3: {
                              matr_idx_map[3] = i2;
                              int_v.emplace_back();
                              f1(int_v.back(),  
                                 int_vec2[i]); 
                              break;
                            }
                 case 4: {
                              matr_idx_map[4] = i2;
                              uint_v.emplace_back();
                              f1(uint_v.back(),  
                                 uint_vec2[i]); 
                              break;
                            }
                  case 5: {
                              matr_idx_map[5] = i2;
                              dbl_v.emplace_back();
                              f1(dbl_v.back(),  
                                 dbl_vec2[i]); 
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
            cols_proceed(copy_col_dense, copy_col);
        }
    }

    if (!name_v_row2.empty())
        copy_col(name_v_row, name_v_row2);

    if constexpr (!IsDense) {
        if (in_view)
            copy_view();
    } else {
        if (in_view)
            copy_view_dense();
    }

}







