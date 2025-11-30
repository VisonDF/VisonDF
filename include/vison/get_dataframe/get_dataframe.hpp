#pragma once

void get_dataframe(const std::vector<size_t>& cols, 
                   Dataframe& cur_obj)
{
    nrow = cur_obj.get_nrow();

    if (cols.empty()) {
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
        tmp_val_refv.resize(ncol);
        for (auto& el : tmp_val_refv) {
          el.reserve(nrow);
        };

        size_t i2 = 0;
        for (int i : cols) {

            tmp_val_refv[i2].assign(cur_tmp[i].begin(), cur_tmp[i].end());

            switch (type_refv1[i]) {
                case 's': {
                               matr_idx[0].push_back(i2);
                               str_v.emplace_back();
                               str_v.back().resize(nrow);
                               auto* __restrict dst = str_v.back().data();
                               const auto* __restrict src = str_v2[i].data();
                               for (size_t i = 0; i < nrow; ++i)
                                   dst[i] = src[i];
                               break;
                          }
                case 'c': {
                               matr_idx[1].push_back(i2);
                               chr_v.emplace_back();
                               chr_v.back().resize(nrow);
                               auto* __restrict dst = chr_v.back().data();
                               const auto* __restrict src = chr_v2[i].data();
                               memcpy(dst,
                                      src,
                                      nrow * sizeof(CharT)
                                     );
                               break;
                          }
                case 'b': {
                               matr_idx[2].push_back(i2);
                               bool_v.emplace_back();
                               bool_v.back().resize(nrow);
                               auto* __restrict dst = bool_v.back().data();
                               const auto* __restrict src = bool_v2[i].data();
                               memcpy(dst,
                                      src,
                                      nrow * sizeof(uint8_t)
                                     );
                               break;
                          }
                case 'i': {
                               matr_idx[3].push_back(i2);
                               int_v.emplace_back();
                               int_v.back().resize(nrow);
                               auto* __restrict dst = int_v.back().data();
                               const auto* __restrict src = int_v2[i].data();
                               memcpy(dst,
                                      src,
                                      nrow * sizeof(IntT)
                                     );
                               break;
                          }
                case 'u': {
                               matr_idx[4].push_back(i2);
                               uint_v.emplace_back();
                               uint_v.back().resize(nrow);
                               auto* __restrict dst = uint_v.back().data();
                               const auto* __restrict src = uint_v2[i].data();
                               memcpy(dst,
                                      src,
                                      nrow * sizeof(UIntT)
                                     );
                               break;
                          }
                case 'd': {
                               matr_idx[5].push_back(i2);
                               dbl_v.emplace_back();
                               dbl_v.back().resize(nrow);
                               auto* __restrict dst = dbl_v.back().data();
                               const auto* __restrict src = dbl_v2[i].data();
                               memcpy(dst,
                                      src,
                                      nrow * sizeof(FloatT)
                                     );
                               break;
                          }
            }

            name_v[i2]    = name_v1[i];
            type_refv[i2] = type_refv1[i];
            i2 += 1;

        }

    }

    name_v_row = cur_obj.get_rowname();
}



