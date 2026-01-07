#pragma once

template <unsigned int CORES = 4,
          bool NUMA = false,
          bool IsDense = false // assumed sorted
        >
void get_dataframe_filter_idx_mt(const std::vector<size_t>& cols, 
                                 Dataframe& cur_obj,
                                 const std::vector<unsigned int>& mask)
{

    const unsigned int local_nrow = mask.size();
    nrow = local_nrow;

    in_view = cur_obj.get_in_view();
    ankerl::unordered_dense::set<unsigned int>& pre_col_alrd_materialized  = cur_obj.get_col_alrd_materialized();
    row_view_idx2           = cur_obj.get_row_view_idx();
    row_view_map2           = cur_obj.get_row_view_map();

    if (in_view) {
        row_view_idx.resize(local_nrow);
        row_view_map.reserve(local_nrow);
    }

    auto copy_col_dense = [&mask, 
                           local_nrow]<typename T>(
                                                   const std::vector<T>& src_vec2,
                                                   std::vector<T>& dst_vec
                                                  )
    {
        const std::string* __restrict src = src_vec2.data();
        std::string*       __restrict dst = dst_vec.data();

        std::vector<RunsIdxMt> runs;
        runs.reserve(mask.size() / 3);
        
        for (size_t i = 0; i < mask.size();) {
            size_t start = i;
            size_t src_start = mask[i];
        
            while (i + 1 < mask.size() &&
                   mask[i + 1] == mask[i] + 1) {
                ++i;
            }
        
            runs.push_back({start, src_start, i - start + 1});
            ++i;
        }

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
                            runs.size(), 
                            tid, 
                            nthreads, 
                            numa_nodes);
                } else {
                    simple_mt(cur_struct,
                              runs.size(), 
                              tid, 
                              nthreads);
                }
                    
                const unsigned int start = cur_struct.start;
                const unsigned int end   = cur_struct.end;

                for (size_t r = start; r < end; ++r) {
                    const auto& run = runs[r]; 
                    std::memcpy(dst.data() + run.mask_pos,
                                src.data() + run.src_start,
                                run.len * sizeof(T));
                }

            }

        } else {

            for (size_t r = 0; r < local_nrow; ++r) {
                const auto& run = runs[r]; 
                std::memcpy(dst.data() + run.mask_pos,
                            src.data() + run.src_start,
                            run.len * sizeof(T));
            }

        }
    };

    auto copy_col_view_mt = [&mask, 
                             &row_view_idx2, 
                             &row_view_map2,
                             local_nrow]<typename T>(
                                                      const auto& src_vec2,
                                                      auto& dst_vec
                                                    )
    {
        const std::string* __restrict src = src_vec2.data();
        std::string*       __restrict dst = dst_vec.data();

        std::vector<RunsIdxMt> runs;
        runs.reserve(mask.size() / 3);
        
        for (size_t i = 0; i < local_nrow;) {
            size_t start = i;
            size_t src_start = mask[i];
        
            while (i + 1 < mask.size() &&
                   mask[i + 1] == mask[i] + 1) {
                ++i;
            }
        
            runs.push_back({start, src_start, i - start + 1});
            ++i;
        }

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
                    
                const unsigned int start = cur_struct.start;
                const unsigned int len   = cur_struct.len;

                for (size_t r = start; r < end; ++r) {
                    const auto& run = runs[r]; 
                    std::memcpy(dst.data() + run.mask_pos,
                                src.data() + run.src_start,
                                run.len * sizeof(T));
                    std::memcpy(row_view_idx.data()  + run.mask_pos,
                                row_view_idx2.data() + run.src_start,
                                run.len * sizeof(T));

                    for (size_t i = run.src_start; i < run.src_start + len; ++i)
                        row_view_map[row_view_idx2[i]] = i;

                }

            }

        } else {

            for (size_t r = 0; r < local_nrow; ++r) {
                const auto& run = runs[r]; 
                std::memcpy(dst.data() + run.mask_pos,
                            src.data() + run.src_start,
                            run.len * sizeof(T));
                std::memcpy(row_view_idx.data()  + run.mask_pos,
                            row_view_idx2.data() + run.src_start,
                            run.len * sizeof(T));

            }

            for (size_t i = 0; i < row_view_idx.size(); ++i)
                row_view_map[row_view_idx[i]] = i;

        }

    };

    auto copy_col = [&mask, 
                    local_nrow](
                                const auto& src_vec2,
                                auto& dst_vec
                               )
    {
        const std::string* __restrict src = src_vec2.data();
        std::string*       __restrict dst = dst_vec.data();
   
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
                    
                const unsigned int start = cur_struct.start;
                const unsigned int end   = cur_struct.end;

                for (size_t j = start; j < end; ++j) {
                    const size_t act = mask[j];
                    dst[j]     = src[act];
                }

            }

        } else {

            for (size_t j = 0; j < local_nrow; ++j) {
                const size_t act = mask[j];
                dst[j]     = src[act];
            }

        }
    };

    auto copy_col_view = [&mask, 
                          &row_view_idx2, 
                          &row_view_map2,
                          local_nrow](
                                       const auto& src_vec2,
                                       auto& dst_vec
                                     )
    {
        const std::string* __restrict src = src_vec2.data();
        std::string*       __restrict dst = dst_vec.data();
   
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
                    
                const unsigned int start = cur_struct.start;
                const unsigned int end   = cur_struct.end;

                for (size_t j = start; j < end; ++j) {
                    const size_t act = mask[j];
                    dst[j]     = src[act];
                    row_view_idx[j] = row_view_idx2[act];
                    row_view_map[row_view_idx2[act]] = j;
                }

            }

        } else {

            for (size_t j = 0; j < local_nrow; ++j) {
                const size_t act = mask[j];
                dst[j]     = src[act];
                row_view_idx[j] = row_view_idx2[act];
                row_view_map[row_view_idx2[act]] = j;
            }

        }
    };

    const auto& str_vec2  = cur_obj.get_str_vec();
    const auto& chr_vec2  = cur_obj.get_chr_vec();
    const auto& bool_vec2 = cur_obj.get_bool_vec();
    const auto& int_vec2  = cur_obj.get_int_vec();
    const auto& uint_vec2 = cur_obj.get_uint_vec();
    const auto& dbl_vec2  = cur_obj.get_dbl_vec();

    auto process_container = [local_nrow](const auto& matr){
        for (const auto& el : matr) {
            matr.emplace_back();
            matr.back().resize(local_nrow);
            auto* dst       = matr_v.back().data();
            const auto* src = el.data();
            copy_col(dst, src);
        }
    };

    auto process_container_view = [local_nrow](const auto& matr){
        for (const auto& el : matr) {
            matr.emplace_back();
            matr.back().resize(local_nrow);
            auto* dst       = matr_v.back().data();
            const auto* src = el.data();
            copy_col_view(dst, src);
        }
    };

    auto cols_proceed = [local_nrow, 
                         &pre_col_alrd_materialized](auto&& f) 
    {
        size_t i2 = 0;
        for (int i : cols) {

            switch (type_refv[i]) {
                  case 's': {

                              matr_idx_map[0] = i2;
                              str_v.emplace_back();
                              str_v.back().resize(local_nrow);
                              f(str_vec2[i],  
                                str_v.back()); 
                              break;
                            }
                  case 'c': {
                              chr_v.emplace_back();
                              chr_v.back().resize(local_nrow);
                              f(chr_vec2[i],  
                                chr_v.back()); 
                              break;
                            }
                  case 'b': {
                              bool_v.emplace_back();
                              bool_v.back().resize(local_nrow);
                              f(bool_vec2[i],  
                                bool_v.back()); 
                              break;
                            }
                  case 'i': {
                              int_v.emplace_back();
                              int_v.back().resize(local_nrow);
                              f(int_vec2[i],  
                                int_v.back()); 
                              break;
                            }
                 case 'u': {
                              uint_v.emplace_back();
                              uint_v.back().resize(local_nrow);
                              f(uint_vec2[i],  
                                uint_v.back()); 
                              break;
                            }
                  case 'd': {
                              dbl_v.emplace_back();
                              dbl_v.back().resize(local_nrow);
                              f(dbl_vec2[i],  
                                dbl_v.back()); 
                              break;
                            }
            }

            if (pre_col_alrd_materialized.contains(i))
                col_alrd_materialized.insert(i);
                
            name_v[i2]    = name_v1[i];
            type_refv[i2] = type_refv1[i];

            i2 += 1;

        }

    };

    if (cols.empty()) {

        col_ard_materialized = pre_col_alrd_materialized;
        matr_idx     = cur_obj.get_matr_idx();
        matr_idx_map = cur_obj.get_matr_idx_map();
        sync_map_col = cur_obj.get_sync_map_col();
        ncol         = cur_obj.get_ncol();

        const auto& str_v2  = cur_obj.get_str_vec();

        if (!in_view) {

            const auto& str_v2  = cur_obj.get_str_vec();
            process_container_view(str_v2);

            const auto& chr_v2  = cur_obj.get_chr_vec();
            process_container(chr_v2);

            const auto& bool_v2  = cur_obj.get_bool_vec();
            process_container(bool_v2);

            const auto& int_v2  = cur_obj.get_int_vec();
            process_container(int_v2);

            const auto& uint_v2  = cur_obj.get_uint_vec();
            process_container(uint_v2);

            const auto& dbl_v2  = cur_obj.get_dbl_vec();
            process_container(dbl_v2);

        } else {

            const auto& str_v2  = cur_obj.get_str_vec();
            process_container_view(str_v2);

            const auto& chr_v2  = cur_obj.get_chr_vec();
            process_container_view(chr_v2);

            const auto& bool_v2  = cur_obj.get_bool_vec();
            process_container_view(bool_v2);

            const auto& int_v2  = cur_obj.get_int_vec();
            process_container_view(int_v2);

            const auto& uint_v2  = cur_obj.get_uint_vec();
            process_container_view(uint_v2);

            const auto& dbl_v2  = cur_obj.get_dbl_vec();
            process_container_view(dbl_v2);

        }

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

        if (!in_view) {

            cols_proceed(copy_col);

        } else {

            cols_proceed(copy_col_view);

        }

    }

}



