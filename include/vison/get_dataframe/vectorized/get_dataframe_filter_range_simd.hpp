#pragma once

void get_dataframe_filter_range_simd(const std::vector<int>& cols, 
                Dataframe& cur_obj,
                const std::vector<uint8_t>& mask,
                unsigned int& strt_vl)
{

    const size_t tot_nrow = cur_obj.get_nrow();

    std::vector<size_t> active_rows;
    active_rows.reserve(tot_nrow);
    const std::vector<std::string>& name_v_row2 = cur_obj.get_rowname();

    if (name_v_row2.empty()) {
      for (size_t r = 0; r < tot_nrow; ++r)
          if (mask[r]) active_rows.push_back(strt_vl + r);
    } else {
      name_v_row.reserve(tot_nrow);
      for (size_t r = 0; r < tot_nrow; ++r)
          if (mask[r]) {
            active_rows.push_back(strt_vl + r);
            name_v_row.push_back(name_v_row2[r]);
          };
    }
   
    nrow = active_rows.size();

    size_t str_idx = 0, chr_idx = 0, bool_idx = 0;
    size_t int_idx = 0, uint_idx = 0, dbl_idx = 0;

    auto process_string = [&](const auto& src_vec2,
                              auto& dst_vec,
                              size_t& idx_counter,
                              const std::vector<std::string>& cur_tmp2,
                              std::vector<std::string>& refv_tmp)
    {
        const size_t pos_idx  = idx_counter * tot_nrow;
        const size_t base_idx = dst_vec.size();
        dst_vec.resize(base_idx + nrow);
    
        const std::string* __restrict src = src_vec2.data() + pos_idx;
        std::string* __restrict dst       = dst_vec.data() + base_idx;
    
        for (size_t j = 0; j < nrow; ++j) {
            const size_t act = active_rows[j];
            dst[j]     = src[act];
            refv_tmp[j] = cur_tmp2[act];
        }
    
        ++idx_counter;
    };

    auto process_block = [&](auto& src_vec2,
                             auto& dst_vec,
                             size_t& idx_counter,
                             const std::vector<std::string>& cur_tmp2,
                             std::vector<std::string>& refv_tmp) 
    {
       
        using T = typename std::remove_reference_t<decltype(dst_vec)>::value_type;
    
        const size_t pos_idx  = idx_counter * tot_nrow;
        const size_t base_idx = dst_vec.size();
        dst_vec.resize(base_idx + nrow);
    
        const T* __restrict src = src_vec2.data() + pos_idx;
        T* __restrict dst       = dst_vec.data() + base_idx;
    
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
                            process_string(str_vec2,  
                                           str_v,  
                                           str_idx, 
                                           cur_tmp2, 
                                           refv_tmp); break;
                          }
                case 'c': {
                            process_block(chr_vec2,  
                                          chr_v,  
                                          chr_idx, 
                                          cur_tmp2, 
                                          refv_tmp); break;
                          }
                case 'b': {
                            process_block(bool_vec2,  
                                          bool_v,  
                                          bool_idx, 
                                          cur_tmp2, 
                                          refv_tmp); break;
                          }
                case 'i': {
                            process_block(int_vec2,  
                                          int_v,  
                                          int_idx, 
                                          cur_tmp2, 
                                          refv_tmp); break;
                          }
               case 'u': {
                            process_block(uint_vec2,  
                                          uint_v,  
                                          uint_idx, 
                                          cur_tmp2, 
                                          refv_tmp); break;
                          }
               case 'd': {
                            process_block(dbl_vec2,  
                                          dbl_v,  
                                          dbl_idx, 
                                          cur_tmp2, 
                                          refv_tmp); break;
                          }
          }
        }

    }
    else {
        ncol = cols.size();

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
                            process_string(str_vec2,  
                                           str_v,  
                                           str_idx, 
                                           cur_tmp2, 
                                           refv_tmp); break;
                          }
                case 'c': {
                            process_block(chr_vec2,  
                                          chr_v,  
                                          chr_idx, 
                                          cur_tmp2, 
                                          refv_tmp); break;
                          }
                case 'b': {
                            process_block(bool_vec2,  
                                          bool_v,  
                                          bool_idx, 
                                          cur_tmp2, 
                                          refv_tmp); break;
                          }
                case 'i': {
                            process_block(int_vec2,  
                                          int_v,  
                                          int_idx, 
                                          cur_tmp2, 
                                          refv_tmp); break;
                          }
               case 'u': {
                            process_block(uint_vec2,  
                                          uint_v,  
                                          uint_idx, 
                                          cur_tmp2, 
                                          refv_tmp); break;
                          }
               case 'd': {
                            process_block(dbl_vec2,  
                                          dbl_v,  
                                          dbl_idx, 
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



