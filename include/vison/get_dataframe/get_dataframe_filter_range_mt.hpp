#pragma once

template <unsigned int CORES = 4,
          bool NUMA = false,
          bool IsDense = false
    >
void get_dataframe_filter_range_mt(const std::vector<size_t>& cols, 
                                   Dataframe& cur_obj,
                                   const std::vector<uint8_t>& mask,
                                   const size_t strt_vl,
                                   std::vector<RunsIdxMt>& runs = {},
                                   OffsetBoolMask& offset_start = {})
{

    const unsigned int n_el       = mask.size();
    const unsigned int local_nrow = active_rows;
    nrow = local_nrow;

    in_view = cur_obj.get_in_view();
    ankerl::unordered_dense::set<unsigned int>& col_alrd_materialized2  = cur_obj.get_col_alrd_materialized();
    const auto row_view_idx2           = cur_obj.get_row_view_idx();

    if (in_view) {
        row_view_idx.resize(local_nrow);
    }

    auto copy_col_dense = [&mask, 
                           &runs,
                           &offset_start]<typename T>(
                                                       const std::vector<T>& src_vec2,
                                                       std::vector<T>& dst_vec
                                                      )
    {

        if (runs.empty()) {
            runs.reserve(mask.size() / 3); 
            size_t out_idx = 0;
            for (size_t i = 0; i < mask.size();) {
                size_t start = out_idx;
                size_t src_start = i;
            
                while (i + 1 < mask.size() && mask[i]) {
                    ++i;
                    ++out_idx;
                }
            
                runs.push_back({start, strt_vl + src_start, i - start + 1});
                ++i;
            }
            dst_vec.resize(out_idx);
        }

        if (!runs.empty()) {
            if (offset_start.vec.empty()) {
                size_t active_count = 0;
                for (size_t i = 0; i < n_el; ++i) {
                    active_count += mask[i] != 0;
                }
                dst_vec.resize(active_count);
                offset_start.x = active_count;
            } else if (!runs.empty()) {
                dst_vec.resize(offset_start.x);
            }
        }

        const std::string* __restrict src = src_vec2.data();
        std::string*       __restrict dst = dst_vec.data();

        if constexpr (CORES > 1) {

            if (CORES > runs.size())
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

            for (size_t r = 0; r < runs.size(); ++r) {
                const auto& run = runs[r]; 
                std::memcpy(dst.data() + run.mask_pos,
                            src.data() + run.src_start,
                            run.len * sizeof(T));
            }

        }
    };

    auto copy_col_view_dense = [&mask, 
                                &row_view_idx2, 
                                &runs,
                                &offset_start]<typename T>(
                                                             const auto& src_vec2,
                                                             auto& dst_vec
                                                           )
    {

        if (runs.empty()) {
            runs.reserve(mask.size() / 3); 
            size_t out_idx = 0;
            for (size_t i = 0; i < mask.size();) {
                size_t start = out_idx;
                size_t src_start = i;
            
                while (i + 1 < mask.size() && mask[i]) {
                    ++i;
                    ++out_idx;
                }
            
                runs.push_back({start, strt_vl + src_start, i - start + 1});
                ++i;
            }
            dst_vec.resize(out_idx);
            row_view_idx.resize(out_idx);
        }

        if (!runs.empty()) {
            if (offset_start.vec.empty()) {
                size_t active_count = 0;
                for (size_t i = 0; i < n_el; ++i) {
                    active_count += mask[i] != 0;
                }
                dst_vec.resize(active_count);
                row_view_idx.resize(active_count);
                offset_start.x = active_count;
            } else if (!runs.empty()) {
                row_view_idx.resize(offset_start.x);
                dst_vec.resize(offset_start.x);
            }
        }

        const std::string* __restrict src = src_vec2.data();
        std::string*       __restrict dst = dst_vec.data();

        if constexpr (CORES > 1) {

            if (CORES > runs.size())
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
                const unsigned int len   = cur_struct.len;

                for (size_t r = start; r < end; ++r) {
                    const auto& run = runs[r]; 
                    std::memcpy(dst.data() + run.mask_pos,
                                src.data() + run.src_start,
                                run.len * sizeof(T));
                    std::memcpy(row_view_idx.data()  + run.mask_pos,
                                row_view_idx2.data() + run.src_start,
                                run.len * sizeof(T));

                }

            }

        } else {

            for (size_t r = 0; r < runs.size(); ++r) {
                const auto& run = runs[r]; 
                std::memcpy(dst.data() + run.mask_pos,
                            src.data() + run.src_start,
                            run.len * sizeof(T));
                std::memcpy(row_view_idx.data()  + run.mask_pos,
                            row_view_idx2.data() + run.src_start,
                            run.len * sizeof(T));

            }

        }

    };

    auto copy_col = [&mask, 
                     &offset_start](
                                     const auto& src_vec2,
                                     auto& dst_vec
                                    )
    {
        const std::string* __restrict src = src_vec2.data();
        std::string*       __restrict dst = dst_vec.data();
   
        if constexpr (CORES > 1) {

            if (CORES > mask.size())
                throw std::runtime_error("Too much cores for so little nrows\n");

            size_t active_count = 0;
            if (offset_start.vec.empty()) {
                offset_start.vec.reserve(n_el / 3);
                for (size_t i = 0; i < n_el; ++i) {
                    active_count += mask[i] != 0;
                    offset_start.vec.push_back(active_count);
                }
                dst_vec.resize(active_count);
                offset_start.x = active_count;
            } else {
                dst_vec.resize(offset_start.x);
            }

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

                size_t out_idx = offset_start.vec[start];

                for (size_t j = start; j < end; ++j) {
                    if (!mask[j]) continue;
                    dst[out_idx++]     = src[j];
                }

            }

        } else {

            for (size_t j = 0; j < local_nrow; ++j) {
                if (!mask[j]) continue;
                dst.push_back(src[act]);
            }

        }
    };

    auto copy_col_view = [&mask, 
                          &row_view_idx2, 
                          &offset_start,
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

            size_t active_count = 0;
            if (offset_start.vec.empty()) {
                offset_start.vec.reserve(n_el / 3);
                for (size_t i = 0; i < n_el; ++i) {
                    active_count += mask[i] != 0;
                    offset_start.vec.push_back(active_count);
                }
                dst_vec.resize(active_count);
                row_view_idx.resize(offset_start.x);
                offset_start.x = active_count;
            } else {
                dst_vec.resize(offset_start.x);
                row_view_idx.resize(offset_start.x);
            }

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

                size_t out_idx = offset_start.vec[start];

                for (size_t j = start; j < end; ++j) {
                    if (!mask[j]) continue;
                    dst[out_idx]          = src[j];
                    row_view_idx[out_idx] = row_view_idx2[j];
                }

            }

        } else {

            for (size_t j = 0; j < local_nrow; ++j) {
                if (!mask[j]) continue;
                dst.push_back(src[j]);
                row_view_idx.push_back(row_view_idx2[j]);
            }

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
            auto* dst       = matr_v.back().data();
            const auto* src = el.data();
            if constexpr (!IsDense || std::is_same_v<T, std::string>) {
                copy_col(dst, src);
            } else {
                copy_col_dense(dst, src);
            }
        }
    };

    auto process_container_view = [local_nrow]<typename T>(const std::vector<std::vector<T>>& matr2,
                                                           std::vector<std::vector<T>>& matr){
        for (const auto& el : matr2) {
            matr.emplace_back();
            auto* dst       = matr_v.back().data();
            const auto* src = el.data();
            if constexpr (!IsDense || std::is_same_v<T, std::string>) {
                copy_col_view(dst, src);
            } else {
                copy_col_view_dense(dst, src);
            }
        }
    };

    auto cols_proceed = [local_nrow, 
                         &col_alrd_materialized2,
                         &cols](auto&& f1, auto&& f2) 
    {
        size_t i2 = 0;
        for (int i : cols) {

            switch (type_refv[i]) {
                  case 's': {

                              matr_idx_map[0] = i2;
                              str_v.emplace_back();
                              f2(str_vec2[i],  
                                 str_v.back()); 
                              break;
                            }
                  case 'c': {
                              chr_v.emplace_back();
                              f1(chr_vec2[i],  
                                 chr_v.back()); 
                              break;
                            }
                  case 'b': {
                              bool_v.emplace_back();
                              f1(bool_vec2[i],  
                                 bool_v.back()); 
                              break;
                            }
                  case 'i': {
                              int_v.emplace_back();
                              f1(int_vec2[i],  
                                 int_v.back()); 
                              break;
                            }
                 case 'u': {
                              uint_v.emplace_back();
                              f1(uint_vec2[i],  
                                 uint_v.back()); 
                              break;
                            }
                  case 'd': {
                              dbl_v.emplace_back();
                              f1(dbl_vec2[i],  
                                 dbl_v.back()); 
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

        if (!in_view) {

            process_container(str_v2,  str_v);
            process_container(chr_v2,  chr_v);
            process_container(bool_v2, bool_v);
            process_container(int_v2,  int_v);
            process_container(uint_v2, uint_v);
            process_container(dbl_v2,  dbl_v);

        } else {

            process_container_view(str_v2,  str_v);
            process_container_view(chr_v2,  chr_v);
            process_container_view(bool_v2, bool_v);
            process_container_view(int_v2,  int_v);
            process_container_view(uint_v2, uint_v);
            process_container_view(dbl_v2,  dbl_v);

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

            if constexpr (!IsDense) {

                cols_proceed(copy_col, copy_col);

            } else {

                cols_proceed(copy_col_dense, copy_col);

            }

        } else {

            if constexpr (!IsDense) {

                cols_proceed(copy_col_view, copy_col_view);

            } else {

                cols_proceed(copy_col_view_dense, copy_col_view);

            }

        }

    }

}



