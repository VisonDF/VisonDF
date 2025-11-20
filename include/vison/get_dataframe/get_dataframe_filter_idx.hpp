#pragma once

void get_dataframe_filter_idx(const std::vector<int>& cols, 
                Dataframe& cur_obj,
                const std::vector<unsigned int>& mask)
{

    nrow = mask.size();

    const auto& tot_nrow = cur_obj.get_nrow();
    const std::vector<std::string>& name_v_row2 = cur_obj.get_rowname();

    size_t str_idx = 0, chr_idx = 0, bool_idx = 0;
    size_t int_idx = 0, uint_idx = 0, dbl_idx = 0;

    const auto& cur_tmp   = cur_obj.get_tmp_val_refv();

    const auto& str_vec2  = cur_obj.get_str_vec();
    const auto& chr_vec2  = cur_obj.get_chr_vec();
    const auto& bool_vec2 = cur_obj.get_bool_vec();
    const auto& int_vec2  = cur_obj.get_int_vec();
    const auto& uint_vec2 = cur_obj.get_uint_vec();
    const auto& dbl_vec2  = cur_obj.get_dbl_vec();

    auto insert_column = [&](auto &dst_vec,
                             const auto &src_vec2,
                             size_t    &type_idx,
                             std::vector<std::string>& refv_tmp,
                             const std::vector<std::string>& cur_tmp2)
    {
        const size_t pos_idx  = type_idx * tot_nrow;
        const size_t base_idx = dst_vec.size();
    
        dst_vec.resize(base_idx + nrow);
    
        const auto *__restrict src = src_vec2.data() + pos_idx;
        auto       *__restrict dst = dst_vec.data() + base_idx;
    
        for (size_t j = 0; j < nrow; ++j) [[likely]] {
            const size_t act_row = mask[j];
            dst[j] = src[act_row];
            refv_tmp[j] = cur_tmp2[act_row];
        }
    
        type_idx++;
    };

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
                      case 's':
                          insert_column(str_v, str_vec2, str_idx, refv_tmp, cur_tmp2);
                          break;

                      case 'c':
                          insert_column(chr_v, chr_vec2, chr_idx, refv_tmp, cur_tmp2);
                          break;

                      case 'b':
                          insert_column(bool_v, bool_vec2, bool_idx, refv_tmp, cur_tmp2);
                          break;

                      case 'i':
                          insert_column(int_v, int_vec2, int_idx, refv_tmp, cur_tmp2);
                          break;

                      case 'u':
                          insert_column(uint_v, uint_vec2, uint_idx, refv_tmp, cur_tmp2);
                          break;

                      case 'd':
                          insert_column(dbl_v, dbl_vec2, dbl_idx, refv_tmp, cur_tmp2);
                          break;
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

                 switch (type_refv[i]) {
                      case 's':
                          insert_column(str_v, str_vec2, str_idx, refv_tmp, cur_tmp2);
                          break;

                      case 'c':
                          insert_column(chr_v, chr_vec2, chr_idx, refv_tmp, cur_tmp2);
                          break;

                      case 'b':
                          insert_column(bool_v, bool_vec2, bool_idx, refv_tmp, cur_tmp2);
                          break;

                      case 'i':
                          insert_column(int_v, int_vec2, int_idx, refv_tmp, cur_tmp2);
                          break;

                      case 'u':
                          insert_column(uint_v, uint_vec2, uint_idx, refv_tmp, cur_tmp2);
                          break;

                      case 'd':
                          insert_column(dbl_v, dbl_vec2, dbl_idx, refv_tmp, cur_tmp2);
                          break;
                }
                   
                name_v[dst_col]    = name_v1[i];
                type_refv[dst_col] = type_refv1[i];

                dst_col += 1;

        }

    }

}



