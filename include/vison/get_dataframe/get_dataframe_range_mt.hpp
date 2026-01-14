#pragma once

template <unsigned int CORES = 4,
          bool NUMA = false>
void get_dataframe_mt(const std::vector<size_t>& cols, 
                      Dataframe& cur_obj,
                      const size_t start,
                      const size_t end)
{

    nrow                   = end - start;
    const unsigned int local_nrow = nrow;
    
    in_view                = cur_obj.get_in_view();
    ankerl::unordered_dense::set<unsigned int>& col_alrd_materialized2  = cur_obj.get_col_alrd_materialized();
    const auto row_view_idx2                                            = cur_obj.get_row_view_idx();
    const std::vector<std::string>& name_v_row2 = cur_obj.get_rowname();

    auto copy_view = [local_nrow,
                      &row_view_idx2]() 
    {

        if constexpr (CORES > 1) {

            if (CORES > local_nrow)
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
                            local_nrow, 
                            tid, 
                            nthreads, 
                            numa_nodes);
                } else {
                    simple_mt(cur_struct,
                              local_nrow, 
                              tid, 
                              nthreads);
                }
                    
                const unsigned int cur_start = cur_struct.start;
                const unsigned int cur_end   = cur_struct.end;
                const unsigned int cur_len   = cur_struct.len;

                memcpy(row_view_idx.data()  + cur_start, 
                       row_view_idx2.data() + start + cur_start, 
                       len * sizeof(T));


            }

        } else {

           memcpy(row_view_idx.data()  + cur_start, 
                  row_view_idx2.data() + start + cur_start, 
                  len * sizeof(T));

        }
    }

    auto str_copy_col = [local_nrow](std::string* dst, 
                                     const std::string* src) {

        if constexpr (CORES > 1) {

            if (CORES > local_nrow)
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
                            local_nrow, 
                            tid, 
                            nthreads, 
                            numa_nodes);
                } else {
                    simple_mt(cur_struct,
                              local_nrow, 
                              tid, 
                              nthreads);
                }
                    
                const unsigned int cur_start = cur_struct.start;
                const unsigned int cur_end   = cur_struct.end;

                for (size_t i = cur_start; i < cur_end; ++i)
                    dst[i] = src[start + i];

            }

        } else {

            for (size_t i = 0; i < local_nrow; ++i)
                dst[i] = src[start + i];

        }
    }

    auto copy_col = [local_nrow]<typename T>(T* dst, 
                                             const T* src) {

        if constexpr (CORES > 1) {

            if (CORES > local_nrow)
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
                            local_nrow, 
                            tid, 
                            nthreads, 
                            numa_nodes);
                } else {
                    simple_mt(cur_struct,
                              local_nrow, 
                              tid, 
                              nthreads);
                }
                    
                const unsigned int cur_start = cur_struct.start;
                const unsigned int cur_len   = cur_struct.len;

                memcpy(dst + cur_start, 
                       src + start + cur_start, 
                       len * sizeof(T));

            }

        } else {

            memcpy(dst, 
                   src + start, 
                   local_nrow * sizeof(T));

        }
    }

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
            if constexpr (!IsDense) {
                str_copy_col(dst, src);
            } else {
                copy_col(dst, src);
            }
        }
    };

    auto cols_proceed = [local_nrow, 
                         &col_alrd_materialized2,
                         &cols](auto&& f, auto&& f2) 
    {
        size_t i2 = 0;
        for (int i : cols) {

            switch (type_refv[i]) {
                  case 's': {

                              matr_idx_map[0] = i2;
                              str_v.emplace_back();
                              str_v.back().resize(local_nrow);
                              f2(str_v.back().data(),  
                                 str_vec2[i].data()); 
                              break;
                            }
                  case 'c': {
                              chr_v.emplace_back();
                              chr_v.back().resize(local_nrow);
                              f1(chr_v.back().data(),  
                                 chr_vec2[i].data()); 
                              break;
                            }
                  case 'b': {
                              bool_v.emplace_back();
                              bool_v.back().resize(local_nrow);
                              f1(bool_v.back().data(),  
                                 bool_vec2[i].data()); 
                              break;
                            }
                  case 'i': {
                              int_v.emplace_back();
                              int_v.back().resize(local_nrow);
                              f1(int_v.back().data(),  
                                 int_vec2[i].data()); 
                              break;
                            }
                 case 'u': {
                              uint_v.emplace_back();
                              uint_v.back().resize(local_nrow);
                              f1(uint_v.back().data(),  
                                 uint_vec2[i].data()); 
                              break;
                            }
                  case 'd': {
                              dbl_v.emplace_back();
                              dbl_v.back().resize(local_nrow);
                              f1(dbl_v.back().data(),  
                                 dbl_vec2[i].data()); 
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

        col_alrd_materialized  = col_alrd_materialized2();
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

    }
    else {
        ncol = cols.size();

        if (in_view)
            col_ard_materialized.reserve(ncol);

        const auto& str_vec2  = cur_obj.get_str_vec();
        const auto& chr_vec2  = cur_obj.get_chr_vec();
        const auto& bool_vec2 = cur_obj.get_bool_vec();
        const auto& int_vec2  = cur_obj.get_int_vec();
        const auto& uint_vec2 = cur_obj.get_uint_vec();
        const auto& dbl_vec2  = cur_obj.get_dbl_vec();

        const auto& name_v1    = cur_obj.get_colname();
        const auto& type_refv1 = cur_obj.get_typecol();

        type_refv.resize(ncol);
        name_v.resize(ncol);

        sync_map_col = {true, true, true, true, true, true};

        cols_proceed(copy_col, str_copy_col);

    }

    name_v_row.resize(local_nrow);
    str_copy_col(name_v_row.data(), name_v_row2.data());

    if (in_view)
        copy_view();
}





