#pragma once

template <unsigned int CORES = 4>
void get_dataframe_mt(const std::vector<size_t>& cols, 
                      Dataframe& cur_obj)
{

    nrow                   = cur_obj.get_nrow();
    in_view                = cur_obj.get_in_view();
    row_view_idx           = cur_obj.get_row_view_idx();
    row_view_map           = cur_obj.get_row_view_map();
    const unsigned int local_nrow = nrow;
    ankerl::unordered_dense::set<unsigned int>& pre_col_alrd_materialized  = cur_obj.get_col_alrd_materialized();

    auto str_copy_col = [local_nrow](std::string* dst, const std::string* src) {

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

                for (size_t i = start; i < end; ++i)
                    dst[i] = src[i];

            }

        } else {

            for (size_t i = 0; i < local_nrow; ++i)
                dst[i] = src[i];

        }
    }

    auto copy_col = []<typename T>(T* dst, const T* src) {

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

                memcpy(dst + start, 
                       src + start, 
                       len * sizeof(T));

            }

        } else {

            memcpy(dst, 
                   src, 
                   local_nrow * sizeof(T));

        }
    }

    if (cols.empty()) {

        col_alrd_materialized  = pre_col_alrd_materialized();
        matr_idx     = cur_obj.get_matr_idx();
        matr_idx_map = cur_obj.get_matr_idx_map();
        sync_map_col = cur_obj.get_sync_map_col();
        ncol         = cur_obj.get_ncol();

        const auto& str_v2  = cur_obj.get_str_vec();
        for (auto& el : str_v2) {
            str_v.emplace_back();
            str_v.back().resize(local_nrow);
            auto* dst       = str_v.back().data();
            const auto* src = el.data();
            str_copy_col(dst, src);
        }

        const auto& chr_v2  = cur_obj.get_chr_vec();
        for (auto& el : chr_v2) {
            chr_v.emplace_back();
            chr_v.back().resize(local_nrow);
            auto* dst       = chr_v.back().data();
            const auto* src = el.data();
            copy_col(dst, src);
        }

        const auto& bool_v2  = cur_obj.get_bool_vec();
        for (auto& el : bool_v2) {
            bool_v.emplace_back();
            bool_v.back().resize(local_nrow);
            auto* dst       = bool_v.back().data();
            const auto* src = el.data();
            copy_col(dst, src);
        }

        const auto& int_v2  = cur_obj.get_int_vec();
        for (auto& el : int_v2) {
            int_v.emplace_back();
            int_v.back().resize(local_nrow);
            auto* dst       = int_v.back().data();
            const auto* src = el.data();
            copy_col(dst, src);
        }

        const auto& uint_v2  = cur_obj.get_uint_vec();
        for (auto& el : uint_v2) {
            uint_v.emplace_back();
            uint_v.back().resize(local_nrow);
            auto* dst       = uint_v.back().data();
            const auto* src = el.data();
            copy_col(dst, src);
        }

        const auto& dbl_v2  = cur_obj.get_dbl_vec();
        for (auto& el : dbl_v2) {
            dbl_v.emplace_back();
            dbl_v.back().resize(local_nrow);
            auto* dst       = dbl_v.back().data();
            const auto* src = el.data();
            copy_col(dst, src);
        }

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

        size_t i2 = 0;
        for (int i : cols) {

            switch (type_refv1[i]) {
                case 's': {
                               matr_idx_map[0][i2] = matr_idx[0].size();
                               matr_idx[0].push_back(i2);
                               str_v.emplace_back();
                               str_v.back().resize(local_nrow);

                               auto* __restrict dst       = str_v.back().data();
                               const auto* __restrict src = str_v2[i].data();

                               str_copy_col(dst, src);

                               break;
                          }
                case 'c': {
                               matr_idx_map[1][i2] = matr_idx[1].size();
                               matr_idx[1].push_back(i2);
                               chr_v.emplace_back();
                               chr_v.back().resize(local_nrow);                               

                               CharT* __restrict dst       = chr_v.back().data();
                               const CharT* __restrict src = chr_v2[i].data();

                               copy_col(dst, src);

                               break;
                          }
                case 'b': {
                               matr_idx_map[2][i2] = matr_idx[2].size();
                               matr_idx[2].push_back(i2);
                               bool_v.emplace_back();
                               bool_v.back().resize(local_nrow);                               

                               uint8_t* __restrict dst       = bool_v.back().data();
                               const uint8_t* __restrict src = bool_v2[i].data();

                               copy_col(dst, src);

                               break;
                          }
                case 'i': {
                               matr_idx_map[3][i2] = matr_idx[3].size();
                               matr_idx[3].push_back(i2);
                               int_v.emplace_back();
                               int_v.back().resize(local_nrow); 

                               IntT* __restrict dst       = int_v.back().data();
                               const IntT* __restrict src = int_v2[i].data();

                               copy_col(dst, src);

                               break;
                          }
                case 'u': {
                               matr_idx_map[4][i2] = matr_idx[4].size();
                               matr_idx[4].push_back(i2);
                               uint_v.emplace_back();
                               uint_v.back().resize(local_nrow);                               

                               UIntT* __restrict dst       = uint_v.back().data();
                               const UIntT* __restrict src = uint_v2[i].data();

                               copy_col(dst, src);

                               break;
                          }
                case 'd': {
                               matr_idx_map[5][i2] = matr_idx[5].size();
                               matr_idx[5].push_back(i2);
                               dbl_v.emplace_back();
                               dbl_v.back().resize(local_nrow);                               

                               FloatT* __restrict dst       = dbl_v.back().data();
                               const FloatT* __restrict src = dbl_v2[i].data();

                               copy_col(dst, src);

                               break;
                          }
            }

            if (pre_col_alrd_materialized.contains(i))
                col_alrd_materialized.insert(i);

            name_v[i2]    = name_v1[i];
            type_refv[i2] = type_refv1[i];
            i2 += 1;

        }

    }

    name_v_row = cur_obj.get_rowname();
}



