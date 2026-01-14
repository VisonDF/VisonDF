#pragma once

template <unsigned int CORES           = 4,
          bool NUMA                    = false,
          bool IsDense                 = false, // assumed sorted
          bool IsSorted                = true, 
          bool IdxIsTrue               = true,
          AssertionType AssertionLevel = AssertionType::Normal
        >
void get_dataframe_filter_idx_mt(const std::vector<size_t>& cols, 
                                 Dataframe& cur_obj,
                                 const std::vector<unsigned int>& mask,
                                 Runs& runs = Runs{}
                                 )
{

    const size_t nrow2 = cur_obj.get_nrow();

    if constexpr (IsDense && !Sorted) {
        throw std::runtime_error("To use `IsDense` parameter, you must sort the mask\n");
    }

    if constexpr (!IsSorted && !IdxIsTrue) {
        std::sort(mask.begin(), mask.end());
    }

    if constexpr (!IdxIsTrue) {
        if (mask.back() >= nrow2)
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
            if (mask.back() >= nrow2) {
                throw std::runtime_error("mask indices are out of bound\n");
            }
        } else if constexpr (IdxIsTrue) {
            for (auto el : mask) {
                if (el >= nrow2) {
                    throw std::runtime_error("mask indices are out of bound\n");
                }
            }
        }
    }

    const unsigned int local_nrow = () ? mask.size() : nrow2 - mask.size();
    nrow = local_nrow;

    in_view = cur_obj.get_in_view();
    ankerl::unordered_dense::set<unsigned int>& col_alrd_materialized2  = cur_obj.get_col_alrd_materialized();
    const auto row_view_idx2           = cur_obj.get_row_view_idx();

    if (in_view) {
        row_view_idx.resize(local_nrow);
    }

    auto copy_col_dense = [&mask,
                           &runs,
                           local_nrow]<typename T>(
                                                   const std::vector<T>& src_vec2,
                                                   std::vector<T>& dst_vec
                                                  )
    {
        const std::string* __restrict src = src_vec2.data();
        std::string*       __restrict dst = dst_vec.data();

        if constexpr (CORES > 1) {

            if (CORES > local_nrow)
                throw std::runtime_error("Too much cores for so little nrows\n");

            std::vector<size_t> thread_counts;
            if (runs.vec.empty() || runs.thread_offsets.empty()) {

                idx_offset_per_thread_mt<IdxIsTrue>(runs.thread_offsets,
                                                    thread_counts,
                                                    mask,
                                                    CORES,
                                                    local_nrow,
                                                    runs.vec);

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

                if constexpr (IdxIsTrue) {
                    for (size_t r = start; r < start + len; ++r) {
                        const auto& run = runs.vec[r]; 
                        std::memcpy(dst.data() + run.mask_pos,
                                    src.data() + run.src_start,
                                    run.len * sizeof(T));
                    }
                } else {
                    const size_t delta = thread_counts[tid];
                    for (size_t r = start; r < start + len; ++r) {
                        const auto& run = runs[r]; 
                        std::memcpy(dst.data() + delta + run.mask_pos,
                                    src.data() + run.src_start,
                                    run.len * sizeof(T));
                    }
                }
            }

        } else {

            if (runs.empty()) {

                idx_offset_per_thread<IdxIsTrue>(mask,
                                                 local_nrow,
                                                 runs.vec);

            }

            for (size_t r = 0; r < runs.size(); ++r) {
                const auto& run = runs.vec[r]; 
                std::memcpy(dst.data() + run.mask_pos,
                            src.data() + run.src_start,
                            run.len * sizeof(T));
            }

        }
    };

    auto copy_col_view_dense = [&mask, 
                                &runs,
                                &row_view_idx2, 
                                local_nrow]<typename T>(
                                                    const auto& src_vec2,
                                                    auto& dst_vec
                                                 )
    {
        const std::string* __restrict src = src_vec2.data();
        std::string*       __restrict dst = dst_vec.data();

        if constexpr (CORES > 1) {

            if (CORES > local_nrow)
                throw std::runtime_error("Too much cores for so little nrows\n");

            std::vector<size_t> thread_counts;
            if (runs.vec.empty() || runs.thread_offsets.empty()) {

                idx_offset_per_thread_mt<IdxIsTrue>(runs.thread_offsets,
                                                    thread_counts,
                                                    mask,
                                                    CORES,
                                                    local_nrow,
                                                    runs.vec);

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
                const unsigned int len   = cur_struct.len;

                if constexpr (IdxIsTrue) {
                    for (size_t r = start; r < start + len; ++r) {
                        const auto& run = runs.vec[r]; 
                        std::memcpy(dst.data() + run.mask_pos,
                                    src.data() + run.src_start,
                                    run.len * sizeof(T));
                        std::memcpy(row_view_idx.data()  + run.mask_pos,
                                    row_view_idx2.data() + run.src_start,
                                    run.len * sizeof(T));
                    }
                } else {
                    const size_t delta = thread_counts[tid];
                    for (size_t r = start; r < start + len; ++r) {
                        const auto& run = runs[r]; 
                        std::memcpy(dst.data() + delta + run.mask_pos,
                                    src.data() + run.src_start,
                                    run.len * sizeof(T));
                        std::memcpy(row_view_idx.data()  + delta + run.mask_pos,
                                    row_view_idx2.data() + run.src_start,
                                    run.len * sizeof(T));
                    }
                }
            }

        } else {

            if (runs.empty()) {

                idx_offset_per_thread<IdxIsTrue>(mask,
                                                 local_nrow,
                                                 runs.vec);

            }

            for (size_t r = 0; r < runs.size(); ++r) {
                const auto& run = runs.vec[r]; 
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
                     &runs,
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

                if constexpr (IdxIsTrue) {
                    for (size_t i = start; i < end; ++i)
                        dst[i] = src[mask[i]];
                } else {
                    size_t out_idx = runs.thread_offsets[tid];
                    for (size_t i = start; i < end; ++i) {
                        const size_t next_stop = mask[i];
                        while (out_idx < next_stop) {
                            dst[out_idx] = src[out_idx];
                            out_idx += 1;
                        }
                    }
                }

            }

        } else {

            if constexpr (IdxIsTrue) {
                for (size_t i = 0; i < mask.size(); ++i)
                    dst[i] = src[mask[i]];
            } else {
                size_t out_idx = 0;
                for (size_t i = 0; i < mask.size(); ++i) {
                    const size_t next_stop = mask[i];
                    while (out_idx < next_stop) {
                        dst[out_idx] = src[out_idx];
                        out_idx += 1;
                    }
                    out_idx += 1;
                }
            }

        }
    };

    auto copy_col_view = [&mask,
                          &runs,
                          &row_view_idx2, 
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

                if constexpr (IdxIsTrue) {
                    for (size_t i = start; i < end; ++i) {
                        const size_t act = mask[j];
                        dst[j]     = src[act];
                        row_view_idx[j] = row_view_idx2[act];
                    }
                } else {
                    size_t out_idx = runs.thread_offsets[tid];
                    for (size_t i = start; i < end; ++i) {
                        const size_t next_stop = mask[i];
                        while (out_idx < next_stop) {
                            dst[out_idx] = src[out_idx];
                            row_view_idx[out_idx] = row_view_idx2[out_idx];
                            out_idx += 1;
                        }
                    }
                }
            }

        } else {

            if constexpr (IdxIsTrue) {
                for (size_t j = 0; j < local_nrow; ++j) {
                    const size_t act = mask[j];
                    dst[j]     = src[act];
                    row_view_idx[j] = row_view_idx2[act];
                }
            } else {
                size_t out_idx = 0;
                for (size_t i = 0; i < mask.size(); ++i) {
                    const size_t next_stop = mask[i];
                    while (out_idx < next_stop) {
                        dst[out_idx] = src[out_idx];
                        row_view_idx[out_idx] = row_view_idx2[out_idx];
                        out_idx += 1;
                    }
                    out_idx += 1;
                }
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

    auto process_container_view = [local_nrow]<typename T>(const std::vector<std::vector<T>>& matr2,
                                                           std::vector<std::vector<T>>& matr) {
        for (const auto& el : matr2) {
            matr.emplace_back();
            matr.back().resize(local_nrow);
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
                              str_v.back().resize(local_nrow);
                              f2(str_vec2[i],  
                                 str_v.back()); 
                              break;
                            }
                  case 'c': {
                              chr_v.emplace_back();
                              chr_v.back().resize(local_nrow);
                              f1(chr_vec2[i],  
                                 chr_v.back()); 
                              break;
                            }
                  case 'b': {
                              bool_v.emplace_back();
                              bool_v.back().resize(local_nrow);
                              f1(bool_vec2[i],  
                                 bool_v.back()); 
                              break;
                            }
                  case 'i': {
                              int_v.emplace_back();
                              int_v.back().resize(local_nrow);
                              f1(int_vec2[i],  
                                 int_v.back()); 
                              break;
                            }
                 case 'u': {
                              uint_v.emplace_back();
                              uint_v.back().resize(local_nrow);
                              f1(uint_vec2[i],  
                                 uint_v.back()); 
                              break;
                            }
                  case 'd': {
                              dbl_v.emplace_back();
                              dbl_v.back().resize(local_nrow);
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



