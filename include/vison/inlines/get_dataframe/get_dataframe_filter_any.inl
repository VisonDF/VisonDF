#pragma once

void get_dataframe_any(const std::vector<size_t>& cols, 
                Dataframe& cur_obj,
                const std::vector<unsigned int>& active_rows)
{

    auto insert_column = [&](auto &dst_vec,
                             const auto &src_vec2,
                             std::vector<std::string>& refv_tmp,
                             const std::vector<std::string>& cur_tmp2)
    {
        const auto *__restrict src = src_vec2.data();
        auto       *__restrict dst = dst_vec.data();
    
        for (size_t j = 0; j < nrow; ++j) [[likely]] {
            const size_t idx_row = active_rows[j];
            dst[j] = src[idx_row];
            refv_tmp[j] = cur_tmp2[idx_row];
        }
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
                                insert_column(str_v.back(), 
                                              str_vec2[i], 
                                              refv_tmp, 
                                              cur_tmp2);
                                break;
                            }
                  case 'c': {
                                chr_v.emplace_back();
                                chr_v.back().resize(nrow);
                                insert_column(chr_v.back(), 
                                              chr_vec2[i], 
                                              refv_tmp, 
                                              cur_tmp2);
                                break;
                            }

                  case 'b': {
                                bool_v.emplace_back();
                                bool_v.back().resize(nrow);
                                insert_column(bool_v.back(), 
                                              bool_vec2[i], 
                                              refv_tmp, 
                                              cur_tmp2);
                                break;
                            }

                  case 'i': {
                                int_v.emplace_back();
                                int_v.back().resize(nrow);
                                insert_column(int_v.back(), 
                                              int_vec2[i], 
                                              refv_tmp, 
                                              cur_tmp2);
                                break;
                            }

                  case 'u': {
                                uint_v.emplace_back();
                                uint_v.back().resize(nrow);
                                insert_column(uint_v.back(), 
                                              uint_vec2[i], 
                                              refv_tmp, 
                                              cur_tmp2);
                                break;
                            }

                  case 'd': {
                                dbl_v.emplace_back();
                                dbl_v.back().resize(nrow);
                                insert_column(dbl_v.back(), 
                                              dbl_vec2[i], 
                                              refv_tmp, 
                                              cur_tmp2);
                                break;
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

        size_t dst_col = 0;

        for (int i : cols) {

            const std::vector<std::string>& cur_tmp2 = cur_tmp[i];
            std::vector<std::string>& refv_tmp = tmp_val_refv[dst_col];

            switch (type_refv[i]) {
                  case 's': {
                                str_v.emplace_back();
                                str_v.back().resize(nrow);
                                insert_column(str_v.back(), 
                                              str_vec2[i], 
                                              refv_tmp, 
                                              cur_tmp2);
                                break;
                            }
                  case 'c': {
                                chr_v.emplace_back();
                                chr_v.back().resize(nrow);
                                insert_column(chr_v.back(), 
                                              chr_vec2[i], 
                                              refv_tmp, 
                                              cur_tmp2);
                                break;
                            }

                  case 'b': {
                                bool_v.emplace_back();
                                bool_v.back().resize(nrow);
                                insert_column(bool_v.back(), 
                                              bool_vec2[i], 
                                              refv_tmp, 
                                              cur_tmp2);
                                break;
                            }

                  case 'i': {
                                int_v.emplace_back();
                                int_v.back().resize(nrow);
                                insert_column(int_v.back(), 
                                              int_vec2[i], 
                                              refv_tmp, 
                                              cur_tmp2);
                                break;
                            }

                  case 'u': {
                                uint_v.emplace_back();
                                uint_v.back().resize(nrow);
                                insert_column(uint_v.back(), 
                                              uint_vec2[i], 
                                              refv_tmp, 
                                              cur_tmp2);
                                break;
                            }

                  case 'd': {
                                dbl_v.emplace_back();
                                dbl_v.back().resize(nrow);
                                insert_column(dbl_v.back(), 
                                              dbl_vec2[i], 
                                              refv_tmp, 
                                              cur_tmp2);
                                break;
                            }
            }
                
            name_v[dst_col]    = name_v1[i];
            type_refv[dst_col] = type_refv1[i];

            dst_col += 1;

        }

    }

}



