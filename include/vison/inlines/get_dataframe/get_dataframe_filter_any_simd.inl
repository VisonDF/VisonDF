#pragma once

inline void get_dataframe_filter_any_simd(const std::vector<size_t>& cols, 
                                          Dataframe& cur_obj,
                                          const std::vector<unsigned int>& active_rows,
                                          const unsigned int nrow)
{

    auto process_string = [&](const auto& src_vec2,
                              auto& dst_vec,
                              const std::vector<std::string>& cur_tmp2,
                              std::vector<std::string>& refv_tmp)
    {
        const std::string* __restrict src = src_vec2.data();
        std::string* __restrict dst       = dst_vec.data();
    
        for (size_t j = 0; j < nrow; ++j) {
            const size_t act = active_rows[j];
            dst[j]     = src[act];
            refv_tmp[j] = cur_tmp2[act];
        }
    };

    auto process_block = [&](auto& src_vec2,
                             auto& dst_vec,
                             const std::vector<std::string>& cur_tmp2,
                             std::vector<std::string>& refv_tmp) 
    {
       
        using T = typename std::remove_reference_t<decltype(dst_vec)>::value_type;
    
        const T* __restrict src = src_vec2.data();
        T* __restrict dst       = dst_vec.data();
    
    #if defined(__ARM_FEATURE_SVE) || defined(__riscv_vector)
        const size_t width = v2::simd_size<unsigned int>();
    #else
        constexpr size_t width = v2::simd_size<unsigned int>();
    #endif
    
        size_t j = 0;
    
        for (; j + width < nrow; j += width) {
    
            v2::simd<unsigned int> idx(&active_rows[j], v2::element_aligned);
    
            v2::simd<T> vals_src;
    
            for (size_t k = 0; k < width; ++k) {
                const size_t act_row = idx[k];
                vals_src[k] = src[act_row];
            }
    
            vals_src.copy_to(&dst[j], v2::element_aligned);
        }
    
        for (; j < nrow; ++j) {
            const size_t act_row = active_rows[j];
            dst[j] = src[act_row];
        }
    
        for (j = 0; j < nrow; j++) {
            const size_t act_row = active_rows[j];
            refv_tmp[j] = cur_tmp2[act_row];
        }
    
        idx_counter += 1;
    };

    const auto& cur_tmp   = cur_obj.get_tmp_val_refv();

    const auto& str_vec2  = cur_obj.get_str_vec();
    const auto& chr_vec2  = cur_obj.get_chr_vec();
    const auto& bool_vec2 = cur_obj.get_bool_vec();
    const auto& int_vec2  = cur_obj.get_int_vec();
    const auto& uint_vec2 = cur_obj.get_uint_vec();
    const auto& dbl_vec2  = cur_obj.get_dbl_vec();

    if (cols.empty()) {
        matr_idx     = cur_obj.get_matr_idx();
        ncol         = cur_obj.get_ncol();

        type_refv = cur_obj.get_typecol();
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
                              str_v.emplace_back();
                              str_v.back().resize(nrow);
                              process_string(str_vec2[i],  
                                             str_v.back(),  
                                             cur_tmp2, 
                                             refv_tmp); break;
                            }
                  case 'c': {
                              chr_v.emplace_back();
                              chr_v.back().resize(nrow);
                              process_string(chr_vec2[i],  
                                             chr_v.back(),  
                                             cur_tmp2, 
                                             refv_tmp); break;
                            }
                  case 'b': {
                              bool_v.emplace_back();
                              bool_v.back().resize(nrow);
                              process_string(bool_vec2[i],  
                                             bool_v.back(),  
                                             cur_tmp2, 
                                             refv_tmp); break;
                            }
                  case 'i': {
                              int_v.emplace_back();
                              int_v.back().resize(nrow);
                              process_string(int_vec2[i],  
                                             int_v.back(),  
                                             cur_tmp2, 
                                             refv_tmp); break;
                            }
                 case 'u': {
                              uint_v.emplace_back();
                              uint_v.back().resize(nrow);
                              process_string(uint_vec2[i],  
                                             uint_v.back(),  
                                             cur_tmp2, 
                                             refv_tmp); break;
                            }
                  case 'd': {
                              dbl_v.emplace_back();
                              dbl_v.back().resize(nrow);
                              process_string(dbl_vec2[i],  
                                             dbl_v.back(),  
                                             cur_tmp2, 
                                             refv_tmp); break;
                            }
            }
        }

    } else {
        ncol = cols.size();

        const auto& name_v1    = cur_obj.get_colname();
        const auto& type_refv1 = cur_obj.get_typecol();

        type_refv.resize(ncol);
        name_v.resize(ncol);
        tmp_val_refv.resize(ncol);
        for (auto& el : tmp_val_refv) {
          el.resize(nrow);
        }

        size_t dst_col = 0;

        for (int i : cols) {

            const std::vector<std::string>& cur_tmp2 = cur_tmp[i];
            std::vector<std::string>& refv_tmp = tmp_val_refv[dst_col];

            switch (type_refv[i]) {
                  case 's': {
                              str_v.emplace_back();
                              str_v.back().resize(nrow);
                              process_string(str_vec2[i],  
                                             str_v.back(),  
                                             cur_tmp2, 
                                             refv_tmp); break;
                            }
                  case 'c': {
                              chr_v.emplace_back();
                              chr_v.back().resize(nrow);
                              process_string(chr_vec2[i],  
                                             chr_v.back(),  
                                             cur_tmp2, 
                                             refv_tmp); break;
                            }
                  case 'b': {
                              bool_v.emplace_back();
                              bool_v.back().resize(nrow);
                              process_string(bool_vec2[i],  
                                             bool_v.back(),  
                                             cur_tmp2, 
                                             refv_tmp); break;
                            }
                  case 'i': {
                              int_v.emplace_back();
                              int_v.back().resize(nrow);
                              process_string(int_vec2[i],  
                                             int_v.back(),  
                                             cur_tmp2, 
                                             refv_tmp); break;
                            }
                 case 'u': {
                              uint_v.emplace_back();
                              uint_v.back().resize(nrow);
                              process_string(uint_vec2[i],  
                                             uint_v.back(),  
                                             cur_tmp2, 
                                             refv_tmp); break;
                            }
                  case 'd': {
                              dbl_v.emplace_back();
                              dbl_v.back().resize(nrow);
                              process_string(dbl_vec2[i],  
                                             dbl_v.back(),  
                                             cur_tmp2, 
                                             refv_tmp); break;
                            }
            }

                
            name_v[dst_col]    = name_v1[i];
            type_refv[dst_col] = type_refv1[i];

            dst_col += 1;

        }
    }

}


