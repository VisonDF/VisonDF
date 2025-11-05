#pragma once

template <bool UseSimd = true>
void get_dataframe_filter_idx(const std::vector<int>& cols, 
                Dataframe& cur_obj,
                const std::vector<uint8_t>& mask)
{

    nrow = mask.size();

    const auto& tot_nrow = cur_obj.get_nrow();
    const std::vector<std::string>& name_v_row2 = cur_obj.get_rowname();

    size_t str_idx = 0, chr_idx = 0, bool_idx = 0;
    size_t int_idx = 0, uint_idx = 0, dbl_idx = 0;

    if constexpr (UseSimd) {

        if (cols.empty() || cols[0] == -1) {
            matr_idx     = cur_obj.get_matr_idx();
            ncol         = cur_obj.get_ncol();

            type_refv = cur_obj.get_typecol();

            for (auto& el : type_refv) {
                switch (el) {
                    case 's': ++str_idx; break;
                    case 'c': ++chr_idx; break;
                    case 'b': ++bool_idx; break;
                    case 'i': ++int_idx; break;
                    case 'u': ++uint_idx; break;
                    case 'd': ++dbl_idx; break;
                }
            }

            str_v.reserve(str_idx * nrow);
            chr_v.reserve(chr_idx * nrow);
            bool_v.reserve(bool_idx * nrow);
            int_v.reserve(int_idx * nrow);
            uint_v.reserve(uint_idx * nrow);
            dbl_v.reserve(dbl_idx * nrow);

            str_idx = 0, chr_idx = 0, bool_idx = 0;
            int_idx = 0, uint_idx = 0, dbl_idx = 0;

            const auto& cur_tmp   = cur_obj.get_tmp_val_refv();

            const auto& str_vec2  = cur_obj.get_str_vec();
            const auto& chr_vec2  = cur_obj.get_chr_vec();
            const auto& bool_vec2 = cur_obj.get_bool_vec();
            const auto& int_vec2  = cur_obj.get_int_vec();
            const auto& uint_vec2 = cur_obj.get_uint_vec();
            const auto& dbl_vec2  = cur_obj.get_dbl_vec();
            
            name_v    = cur_obj.get_colname();
           
            tmp_val_refv.resize(ncol);
            for (auto& el : tmp_val_refv) {
              el.resize(nrow);
            }

            for (size_t i = 0; i < type_refv.size(); i += 1) {

              const std::vector<std::string>& cur_tmp2 = cur_tmp[i];
              std::vector<std::string>& refv_tmp = tmp_val_refv[i];

              switch (type_refv[i]) {
                case 's': {

                            const size_t pos_idx  = str_idx * tot_nrow;

                            const size_t base_idx = str_v.size();
                            str_v.resize(base_idx + nrow);

                            const std::string* __restrict src = str_vec2.data() + pos_idx;
                            std::string* __restrict dst       = str_v.data() + base_idx;

                            for (size_t j = 0; j < nrow; ++j) [[likely]] {
                                const size_t act_row = mask[j];
                                dst[j] = src[act_row];  
                                refv_tmp[j] = cur_tmp2[act_row];
                            }

                            ++str_idx;
                            break;

                              }
                    case 'c': {

                                const size_t pos_idx  = chr_idx * tot_nrow;
                                const size_t base_idx = chr_v.size();
                                chr_v.resize(base_idx + nrow);

                                const double* __restrict src = chr_vec2.data() + pos_idx;
                                double* __restrict dst       = chr_v.data() + base_idx;

                                #if defined(__ARM_FEATURE_SVE) || defined(__riscv_vector)
                                const size_t width = v2::simd_size<unsigned int>();
                                #else
                                constexpr size_t width = v2::simd_size<unsigned int>();
                                #endif

                                size_t j = 0;

                                for (; j + width <= nrow; j += width) {
   
                                    v2::simd<unsigned int> idx(&mask[j], v2::element_aligned);

                                    v2::simd<FloatT> vals_src;

                                    for (size_t k = 0; k < width; ++k) {
                                        const size_t act_row = idx[k];
                                        vals_src[k] = src[act_row];
                                    }

                                    vals_src.copy_to(&dst[j], v2::element_aligned);

                                }

                                for (; j < nrow; ++j) {
                                    const size_t act_row = mask[j];
                                    dst[j] = src[act_row];
                                }

                                for (j = 0; j < nrow; j += 1) {
                                  const size_t act_row = mask[j];
                                  refv_tmp[j] = cur_tmp2[act_row];
                                }

                                chr_idx += 1;
                                break;

                              }
                    case 'b': {

                                    size_t pos_idx = bool_idx * tot_nrow;
                                    
                                    const size_t base_idx = bool_v.size();
                                    bool_v.resize(base_idx + nrow);
                                    
                                    for (size_t j = 0; j < nrow; ++j) [[likely]] {
                                        const size_t act_row = mask[j];
                                        bool_v[base_idx + j] = bool_vec2[pos_idx + act_row];
                                        refv_tmp[j] = cur_tmp2[act_row];
                                    }

                                    bool_idx += 1;

                                    break;

                              }
                    case 'i': {

                                const size_t pos_idx  = int_idx * tot_nrow;
                                const size_t base_idx = int_v.size();
                                int_v.resize(base_idx + nrow);

                                const double* __restrict src = int_vec2.data() + pos_idx;
                                double* __restrict dst       = int_v.data() + base_idx;

                                #if defined(__ARM_FEATURE_SVE) || defined(__riscv_vector)
                                const size_t width = v2::simd_size<unsigned int>();
                                #else
                                constexpr size_t width = v2::simd_size<unsigned int>();
                                #endif

                                size_t j = 0;

                                for (; j + width <= nrow; j += width) {
   
                                    v2::simd<unsigned int> idx(&mask[j], v2::element_aligned);

                                    v2::simd<FloatT> vals_src;

                                    for (size_t k = 0; k < width; ++k) {
                                        const size_t act_row = idx[k];
                                        vals_src[k] = src[act_row];
                                    }

                                    vals_src.copy_to(&dst[j], v2::element_aligned);

                                }

                                for (; j < nrow; ++j) {
                                    const size_t act_row = mask[j];
                                    dst[j] = src[act_row];
                                }

                                for (j = 0; j < nrow; j += 1) {
                                  const size_t act_row = mask[j];
                                  refv_tmp[j] = cur_tmp2[act_row];
                                }

                                int_idx += 1;
                                break;

                              }
                   case 'u': {

                                const size_t pos_idx  = uint_idx * tot_nrow;
                                const size_t base_idx = uint_v.size();
                                uint_v.resize(base_idx + nrow);

                                const double* __restrict src = uint_vec2.data() + pos_idx;
                                double* __restrict dst       = uint_v.data() + base_idx;

                                #if defined(__ARM_FEATURE_SVE) || defined(__riscv_vector)
                                const size_t width = v2::simd_size<unsigned int>();
                                #else
                                constexpr size_t width = v2::simd_size<unsigned int>();
                                #endif

                                size_t j = 0;

                                for (; j + width <= nrow; j += width) {
   
                                    v2::simd<unsigned int> idx(&mask[j], v2::element_aligned);

                                    v2::simd<FloatT> vals_src;

                                    for (size_t k = 0; k < width; ++k) {
                                        const size_t act_row = idx[k];
                                        vals_src[k] = src[act_row];
                                    }

                                    vals_src.copy_to(&dst[j], v2::element_aligned);

                                }

                                for (; j < nrow; ++j) {
                                    const size_t act_row = mask[j];
                                    dst[j] = src[act_row];
                                }

                                for (j = 0; j < nrow; j += 1) {
                                  const size_t act_row = mask[j];
                                  refv_tmp[j] = cur_tmp2[act_row];
                                }

                                uint_idx += 1;
                                break;

                              }
                    case 'd': {

                                const size_t pos_idx  = dbl_idx * tot_nrow;
                                const size_t base_idx = dbl_v.size();
                                dbl_v.resize(base_idx + nrow);

                                const double* __restrict src = dbl_vec2.data() + pos_idx;
                                double* __restrict dst       = dbl_v.data() + base_idx;

                                #if defined(__ARM_FEATURE_SVE) || defined(__riscv_vector)
                                const size_t width = v2::simd_size<unsigned int>();
                                #else
                                constexpr size_t width = v2::simd_size<unsigned int>();
                                #endif

                                size_t j = 0;

                                for (; j + width <= nrow; j += width) {
   
                                    v2::simd<unsigned int> idx(&mask[j], v2::element_aligned);

                                    v2::simd<FloatT> vals_src;

                                    for (size_t k = 0; k < width; ++k) {
                                        const size_t act_row = idx[k];
                                        vals_src[k] = src[act_row];
                                    }

                                    vals_src.copy_to(&dst[j], v2::element_aligned);

                                }

                                for (; j < nrow; ++j) {
                                    const size_t act_row = mask[j];
                                    dst[j] = src[act_row];
                                }

                                for (j = 0; j < nrow; j += 1) {
                                  const size_t act_row = mask[j];
                                  refv_tmp[j] = cur_tmp2[act_row];
                                }

                                dbl_idx += 1;
                                break;

                              }
              }
            }

        }
        else {
            ncol = cols.size();

            const auto& cur_tmp   = cur_obj.get_tmp_val_refv();

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
            tmp_val_refv.resize(ncol);
            for (auto& el : tmp_val_refv) {
              el.resize(nrow);
            }

            size_t i2 = 0;
            for (int i : cols) {
                switch (type_refv1[i]) {
                    case 's': ++str_idx; matr_idx[0].push_back(i2); ++i2; break;
                    case 'c': ++chr_idx; matr_idx[1].push_back(i2); ++i2; break;
                    case 'b': ++bool_idx; matr_idx[2].push_back(i2); ++i2; break;
                    case 'i': ++int_idx; matr_idx[3].push_back(i2); ++i2; break;
                    case 'u': ++uint_idx; matr_idx[4].push_back(i2); ++i2; break;
                    case 'd': ++dbl_idx; matr_idx[5].push_back(i2); ++i2; break;
                }
            }

            str_v.reserve(str_idx * nrow);
            chr_v.reserve(chr_idx * nrow);
            bool_v.reserve(bool_idx * nrow);
            int_v.reserve(int_idx * nrow);
            uint_v.reserve(uint_idx * nrow);
            dbl_v.reserve(dbl_idx * nrow);

            str_idx = 0, chr_idx = 0, bool_idx = 0;
            int_idx = 0, uint_idx = 0, dbl_idx = 0;

            size_t dst_col = 0;

            for (int i : cols) {

                const std::vector<std::string>& cur_tmp2 = cur_tmp[i];
                std::vector<std::string>& refv_tmp = tmp_val_refv[dst_col];

                switch (type_refv1[i]) {
                    case 's': {

                            const size_t pos_idx  = str_idx * tot_nrow;

                            const size_t base_idx = str_v.size();
                            str_v.resize(base_idx + nrow);

                            const std::string* __restrict src = str_vec2.data() + pos_idx;
                            std::string* __restrict dst       = str_v.data() + base_idx;

                            for (size_t j = 0; j < nrow; ++j) [[likely]] {
                                const size_t act_row = mask[j];
                                dst[j] = src[act_row];  
                                refv_tmp[j] = cur_tmp2[act_row];
                            }

                            ++str_idx;
                            break;

                              }
                    case 'c': {

                                const size_t pos_idx  = chr_idx * tot_nrow;
                                const size_t base_idx = chr_v.size();
                                chr_v.resize(base_idx + nrow);

                                const double* __restrict src = chr_vec2.data() + pos_idx;
                                double* __restrict dst       = chr_v.data() + base_idx;

                                #if defined(__ARM_FEATURE_SVE) || defined(__riscv_vector)
                                const size_t width = v2::simd_size<unsigned int>();
                                #else
                                constexpr size_t width = v2::simd_size<unsigned int>();
                                #endif

                                size_t j = 0;

                                for (; j + width <= nrow; j += width) {
   
                                    v2::simd<unsigned int> idx(&mask[j], v2::element_aligned);

                                    v2::simd<FloatT> vals_src;

                                    for (size_t k = 0; k < width; ++k) {
                                        const size_t act_row = idx[k];
                                        vals_src[k] = src[act_row];
                                    }

                                    vals_src.copy_to(&dst[j], v2::element_aligned);

                                }

                                for (; j < nrow; ++j) {
                                    const size_t act_row = mask[j];
                                    dst[j] = src[act_row];
                                }

                                for (j = 0; j < nrow; j += 1) {
                                  const size_t act_row = mask[j];
                                  refv_tmp[j] = cur_tmp2[act_row];
                                }

                                chr_idx += 1;
                                break;

                              }
                    case 'b': {

                                size_t pos_idx = bool_idx * tot_nrow;
                                    
                                const size_t base_idx = bool_v.size();
                                bool_v.resize(base_idx + nrow);
                                
                                for (size_t j = 0; j < nrow; ++j) [[likely]] {
                                    const size_t act_row = mask[j];
                                    bool_v[base_idx + j] = bool_vec2[pos_idx + act_row];
                                    refv_tmp[j] = cur_tmp2[act_row];
                                }

                                bool_idx += 1;
                                
                                break;

                              }
                    case 'i': {

                                const size_t pos_idx  = int_idx * tot_nrow;
                                const size_t base_idx = int_v.size();
                                int_v.resize(base_idx + nrow);

                                const double* __restrict src = int_vec2.data() + pos_idx;
                                double* __restrict dst       = int_v.data() + base_idx;

                                #if defined(__ARM_FEATURE_SVE) || defined(__riscv_vector)
                                const size_t width = v2::simd_size<unsigned int>();
                                #else
                                constexpr size_t width = v2::simd_size<unsigned int>();
                                #endif

                                size_t j = 0;

                                for (; j + width <= nrow; j += width) {
   
                                    v2::simd<unsigned int> idx(&mask[j], v2::element_aligned);

                                    v2::simd<FloatT> vals_src;

                                    for (size_t k = 0; k < width; ++k) {
                                        const size_t act_row = idx[k];
                                        vals_src[k] = src[act_row];
                                    }

                                    vals_src.copy_to(&dst[j], v2::element_aligned);

                                }

                                for (; j < nrow; ++j) {
                                    const size_t act_row = mask[j];
                                    dst[j] = src[act_row];
                                }

                                for (j = 0; j < nrow; j += 1) {
                                  const size_t act_row = mask[j];
                                  refv_tmp[j] = cur_tmp2[act_row];
                                }

                                int_idx += 1;
                                break;

                              }
                   case 'u': {

                                const size_t pos_idx  = uint_idx * tot_nrow;
                                const size_t base_idx = uint_v.size();
                                uint_v.resize(base_idx + nrow);

                                const double* __restrict src = uint_vec2.data() + pos_idx;
                                double* __restrict dst       = uint_v.data() + base_idx;

                                #if defined(__ARM_FEATURE_SVE) || defined(__riscv_vector)
                                const size_t width = v2::simd_size<unsigned int>();
                                #else
                                constexpr size_t width = v2::simd_size<unsigned int>();
                                #endif

                                size_t j = 0;

                                for (; j + width <= nrow; j += width) {
   
                                    v2::simd<unsigned int> idx(&mask[j], v2::element_aligned);

                                    v2::simd<FloatT> vals_src;

                                    for (size_t k = 0; k < width; ++k) {
                                        const size_t act_row = idx[k];
                                        vals_src[k] = src[act_row];
                                    }

                                    vals_src.copy_to(&dst[j], v2::element_aligned);

                                }

                                for (; j < nrow; ++j) {
                                    const size_t act_row = mask[j];
                                    dst[j] = src[act_row];
                                }

                                for (j = 0; j < nrow; j += 1) {
                                  const size_t act_row = mask[j];
                                  refv_tmp[j] = cur_tmp2[act_row];
                                }

                                uint_idx += 1;
                                break;

                              }
                    case 'd': {

                                const size_t pos_idx  = dbl_idx * tot_nrow;
                                const size_t base_idx = dbl_v.size();
                                dbl_v.resize(base_idx + nrow);

                                const double* __restrict src = dbl_vec2.data() + pos_idx;
                                double* __restrict dst       = dbl_v.data() + base_idx;

                                #if defined(__ARM_FEATURE_SVE) || defined(__riscv_vector)
                                const size_t width = v2::simd_size<unsigned int>();
                                #else
                                constexpr size_t width = v2::simd_size<unsigned int>();
                                #endif

                                size_t j = 0;

                                for (; j + width <= nrow; j += width) {
   
                                    v2::simd<unsigned int> idx(&mask[j], v2::element_aligned);

                                    v2::simd<FloatT> vals_src;

                                    for (size_t k = 0; k < width; ++k) {
                                        const size_t act_row = idx[k];
                                        vals_src[k] = src[act_row];
                                    }

                                    vals_src.copy_to(&dst[j], v2::element_aligned);

                                }

                                for (; j < nrow; ++j) {
                                    const size_t act_row = mask[j];
                                    dst[j] = src[act_row];
                                }

                                for (j = 0; j < nrow; j += 1) {
                                  const size_t act_row = mask[j];
                                  refv_tmp[j] = cur_tmp2[act_row];
                                }

                                dbl_idx += 1;
                                break;

                              }
   
                }
                    
                name_v[dst_col]    = name_v1[i];
                type_refv[dst_col] = type_refv1[i];

                dst_col += 1;

            }

        }

    } else if constexpr (!UseSimd) {

        if (cols.empty() || cols[0] == -1) {
            matr_idx     = cur_obj.get_matr_idx();
            ncol         = cur_obj.get_ncol();

            type_refv = cur_obj.get_typecol();

            for (auto& el : type_refv) {
                switch (el) {
                    case 's': ++str_idx; break;
                    case 'c': ++chr_idx; break;
                    case 'b': ++bool_idx; break;
                    case 'i': ++int_idx; break;
                    case 'u': ++uint_idx; break;
                    case 'd': ++dbl_idx; break;
                }
            }

            str_v.reserve(str_idx * nrow);
            chr_v.reserve(chr_idx * nrow);
            bool_v.reserve(bool_idx * nrow);
            int_v.reserve(int_idx * nrow);
            uint_v.reserve(uint_idx * nrow);
            dbl_v.reserve(dbl_idx * nrow);

            str_idx = 0, chr_idx = 0, bool_idx = 0;
            int_idx = 0, uint_idx = 0, dbl_idx = 0;

            const auto& cur_tmp   = cur_obj.get_tmp_val_refv();

            const auto& str_vec2  = cur_obj.get_str_vec();
            const auto& chr_vec2  = cur_obj.get_chr_vec();
            const auto& bool_vec2 = cur_obj.get_bool_vec();
            const auto& int_vec2  = cur_obj.get_int_vec();
            const auto& uint_vec2 = cur_obj.get_uint_vec();
            const auto& dbl_vec2  = cur_obj.get_dbl_vec();
            
            name_v    = cur_obj.get_colname();
           
            tmp_val_refv.resize(ncol);
            for (auto& el : tmp_val_refv) {
              el.resize(nrow);
            }

            for (size_t i = 0; i < type_refv.size(); i += 1) {

              const std::vector<std::string>& cur_tmp2 = cur_tmp[i];
              std::vector<std::string>& refv_tmp = tmp_val_refv[i];

              switch (type_refv[i]) {
                case 's': {

                            const size_t pos_idx  = str_idx * tot_nrow;

                            const size_t base_idx = str_v.size();
                            str_v.resize(base_idx + nrow);

                            const std::string* __restrict src = str_vec2.data() + pos_idx;
                            std::string* __restrict dst       = str_v.data() + base_idx;

                            for (size_t j = 0; j < nrow; ++j) [[likely]] {
                                const size_t act_row = mask[j];
                                dst[j] = src[act_row];  
                                refv_tmp[j] = cur_tmp2[act_row];
                            }

                            ++str_idx;
                            break;

                              }
                    case 'c': {

                                size_t pos_idx = chr_idx * tot_nrow;

                                const size_t base_idx = chr_v.size();
                                chr_v.resize(base_idx + nrow);

                                const auto* __restrict src = chr_vec2.data() + pos_idx;
                                auto* __restrict dst = chr_v.data() + base_idx;
                                
                                for (size_t j = 0; j < nrow; ++j) [[likely]] {
                                    const size_t act_row = mask[j];
                                    dst[j] = src[act_row];
                                    refv_tmp[j] = cur_tmp2[act_row];
                                }

                                chr_idx += 1;
                                break;

                              }
                    case 'b': {

                                    size_t pos_idx = bool_idx * tot_nrow;
                                    
                                    const size_t base_idx = bool_v.size();
                                    bool_v.resize(base_idx + nrow);
                                    
                                    for (size_t j = 0; j < nrow; ++j) [[likely]] {
                                        const size_t act_row = mask[j];
                                        bool_v[base_idx + j] = bool_vec2[pos_idx + act_row];
                                        refv_tmp[j] = cur_tmp2[act_row];
                                    }

                                    bool_idx += 1;

                                    break;

                              }
                    case 'i': {

                                size_t pos_idx = int_idx * tot_nrow;

                                const size_t base_idx = int_v.size();
                                int_v.resize(base_idx + nrow);

                                const auto* __restrict src = int_vec2.data() + pos_idx;
                                auto* __restrict dst = int_v.data() + base_idx;
                                
                                for (size_t j = 0; j < nrow; ++j) [[likely]] {
                                    const size_t act_row = mask[j];
                                    dst[j] = src[act_row];
                                    refv_tmp[j] = cur_tmp2[act_row];
                                }

                                int_idx += 1;
                                break;

                              }
                   case 'u': {

                                size_t pos_idx = uint_idx * tot_nrow;

                                const size_t base_idx = uint_v.size();
                                uint_v.resize(base_idx + nrow);

                                const auto* __restrict src = uint_vec2.data() + pos_idx;
                                auto* __restrict dst = uint_v.data() + base_idx;
                                
                                for (size_t j = 0; j < nrow; ++j) [[likely]] {
                                    const size_t act_row = mask[j];
                                    dst[j] = src[act_row];
                                    refv_tmp[j] = cur_tmp2[act_row];
                                }

                                uint_idx += 1;
                                break;

                              }
                    case 'd': {

                                size_t pos_idx = dbl_idx * tot_nrow;

                                const size_t base_idx = dbl_v.size();
                                dbl_v.resize(base_idx + nrow);

                                const auto* __restrict src = dbl_vec2.data() + pos_idx;
                                auto* __restrict dst = dbl_v.data() + base_idx;
                                
                                for (size_t j = 0; j < nrow; ++j) [[likely]] {
                                    const size_t act_row = mask[j];
                                    dst[j] = src[act_row];
                                    refv_tmp[j] = cur_tmp2[act_row];
                                }

                                dbl_idx += 1;
                                break;

                              }
              }
            }

        }
        else {
            ncol = cols.size();

            const auto& cur_tmp   = cur_obj.get_tmp_val_refv();

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
            tmp_val_refv.resize(ncol);
            for (auto& el : tmp_val_refv) {
              el.resize(nrow);
            }

            size_t i2 = 0;
            for (int i : cols) {
                switch (type_refv1[i]) {
                    case 's': ++str_idx; matr_idx[0].push_back(i2); ++i2; break;
                    case 'c': ++chr_idx; matr_idx[1].push_back(i2); ++i2; break;
                    case 'b': ++bool_idx; matr_idx[2].push_back(i2); ++i2; break;
                    case 'i': ++int_idx; matr_idx[3].push_back(i2); ++i2; break;
                    case 'u': ++uint_idx; matr_idx[4].push_back(i2); ++i2; break;
                    case 'd': ++dbl_idx; matr_idx[5].push_back(i2); ++i2; break;
                }
            }

            str_v.reserve(str_idx * nrow);
            chr_v.reserve(chr_idx * nrow);
            bool_v.reserve(bool_idx * nrow);
            int_v.reserve(int_idx * nrow);
            uint_v.reserve(uint_idx * nrow);
            dbl_v.reserve(dbl_idx * nrow);

            str_idx = 0, chr_idx = 0, bool_idx = 0;
            int_idx = 0, uint_idx = 0, dbl_idx = 0;

            size_t dst_col = 0;

            for (int i : cols) {

                const std::vector<std::string>& cur_tmp2 = cur_tmp[i];
                std::vector<std::string>& refv_tmp = tmp_val_refv[dst_col];

                switch (type_refv1[i]) {
                    case 's': {

                            const size_t pos_idx  = str_idx * tot_nrow;

                            const size_t base_idx = str_v.size();
                            str_v.resize(base_idx + nrow);

                            const std::string* __restrict src = str_vec2.data() + pos_idx;
                            std::string* __restrict dst       = str_v.data() + base_idx;

                            for (size_t j = 0; j < nrow; ++j) [[likely]] {
                                const size_t act_row = mask[j];
                                dst[j] = src[act_row];  
                                refv_tmp[j] = cur_tmp2[act_row];
                            }

                            ++str_idx;
                            break;

                              }
                    case 'c': {

                                size_t pos_idx = chr_idx * tot_nrow;

                                const size_t base_idx = chr_v.size();
                                chr_v.resize(base_idx + nrow);

                                const auto* __restrict src = chr_vec2.data() + pos_idx;
                                auto* __restrict dst = chr_v.data() + base_idx;
                                
                                for (size_t j = 0; j < nrow; ++j) [[likely]] {
                                    const size_t act_row = mask[j];
                                    dst[j] = src[act_row];
                                    refv_tmp[j] = cur_tmp2[act_row];
                                }

                                chr_idx += 1;
                                break;

                              }
                    case 'b': {

                                    size_t pos_idx = bool_idx * tot_nrow;
                                    
                                    const size_t base_idx = bool_v.size();
                                    bool_v.resize(base_idx + nrow);
                                    
                                    for (size_t j = 0; j < nrow; ++j) [[likely]] {
                                        const size_t act_row = mask[j];
                                        bool_v[base_idx + j] = bool_vec2[pos_idx + act_row];
                                        refv_tmp[j] = cur_tmp2[act_row];
                                    }

                                    bool_idx += 1;

                                    break;

                              }
                    case 'i': {

                                size_t pos_idx = int_idx * tot_nrow;

                                const size_t base_idx = int_v.size();
                                int_v.resize(base_idx + nrow);

                                const auto* __restrict src = int_vec2.data() + pos_idx;
                                auto* __restrict dst = int_v.data() + base_idx;
                                
                                for (size_t j = 0; j < nrow; ++j) [[likely]] {
                                    const size_t act_row = mask[j];
                                    dst[j] = src[act_row];
                                    refv_tmp[j] = cur_tmp2[act_row];
                                }

                                int_idx += 1;
                                break;

                              }
                   case 'u': {

                                size_t pos_idx = uint_idx * tot_nrow;

                                const size_t base_idx = uint_v.size();
                                uint_v.resize(base_idx + nrow);

                                const auto* __restrict src = uint_vec2.data() + pos_idx;
                                auto* __restrict dst = uint_v.data() + base_idx;
                                
                                for (size_t j = 0; j < nrow; ++j) [[likely]] {
                                    const size_t act_row = mask[j];
                                    dst[j] = src[act_row];
                                    refv_tmp[j] = cur_tmp2[act_row];
                                }

                                uint_idx += 1;
                                break;

                              }
                    case 'd': {

                                size_t pos_idx = dbl_idx * tot_nrow;

                                const size_t base_idx = dbl_v.size();
                                dbl_v.resize(base_idx + nrow);

                                const auto* __restrict src = dbl_vec2.data() + pos_idx;
                                auto* __restrict dst = dbl_v.data() + base_idx;
                                
                                for (size_t j = 0; j < nrow; ++j) [[likely]] {
                                    const size_t act_row = mask[j];
                                    dst[j] = src[act_row];
                                    refv_tmp[j] = cur_tmp2[act_row];
                                }

                                dbl_idx += 1;
                                break;

                              }
   
                }
                    
                name_v[dst_col]    = name_v1[i];
                type_refv[dst_col] = type_refv1[i];

                dst_col += 1;

            }

        }

    }

}



