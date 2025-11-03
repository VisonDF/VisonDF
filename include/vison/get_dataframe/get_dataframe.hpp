#pragma once

template <typename VecT>
inline void append_block(std::vector<VecT>& dst,
                         const std::vector<VecT>& src,
                         size_t col_idx,
                         size_t nrow)
{
    const size_t start = col_idx * nrow;
    dst.insert(dst.end(), 
                    src.begin() + start, 
                    src.begin() + start + nrow);
}

void get_dataframe(const std::vector<int>& cols, Dataframe& cur_obj)
{
    nrow = cur_obj.get_nrow();

    if (cols.empty() || cols[0] == -1) {
        matr_idx     = cur_obj.get_matr_idx();
        ncol         = cur_obj.get_ncol();
        tmp_val_refv = cur_obj.get_tmp_val_refv();

        str_v  = cur_obj.get_str_vec();
        chr_v  = cur_obj.get_chr_vec();
        bool_v = cur_obj.get_bool_vec();
        int_v  = cur_obj.get_int_vec();
        uint_v = cur_obj.get_uint_vec();
        dbl_v  = cur_obj.get_dbl_vec();

        name_v    = cur_obj.get_colname();
        type_refv = cur_obj.get_typecol();
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

        unsigned int rs_val = ncol * nrow / 2;

        str_v.reserve(rs_val);
        chr_v.reserve(rs_val);
        bool_v.reserve(rs_val);
        int_v.reserve(rs_val);
        uint_v.reserve(rs_val);
        dbl_v.reserve(rs_val);

        size_t str_idx = 0, chr_idx = 0, bool_idx = 0;
        size_t int_idx = 0, uint_idx = 0, dbl_idx = 0;

        for (int i : cols) {
            tmp_val_refv.push_back(cur_tmp[i]);

            switch (type_refv1[i]) {
                case 's': append_block(str_v,  str_vec2,  str_idx,  nrow); ++str_idx ;break;
                case 'c': append_block(chr_v,  chr_vec2,  chr_idx,  nrow); ++chr_idx ; break;
                case 'b': append_block(bool_v, bool_vec2, bool_idx, nrow); ++bool_idx ; break;
                case 'i': append_block(int_v,  int_vec2,  int_idx,  nrow); ++int_idx; break;
                case 'u': append_block(uint_v, uint_vec2, uint_idx, nrow); ++uint_idx; break;
                case 'd': append_block(dbl_v,  dbl_vec2,  dbl_idx,  nrow); ++dbl_idx; break;
            }

            size_t dst_col = tmp_val_refv.size() - 1;
            name_v[dst_col]    = name_v1[i];
            type_refv[dst_col] = type_refv1[i];
        }
    }

    name_v_row = cur_obj.get_rowname();
}



