#pragma once

void rm_row_range_reconstruct(std::vector<unsigned int>& x)
{
    const size_t old_nrow = nrow;
    if (x.empty() || old_nrow == 0) return;

    const size_t new_nrow = old_nrow - x.size();

    std::vector<std::vector<std::string>> new_str_v;  new_str_v .reserve(matr_idx[0].size());
    std::vector<std::vector<CharT>>       new_chr_v;  new_chr_v .reserve(matr_idx[1].size());
    std::vector<std::vector<uint8_t>>     new_bool_v; new_bool_v.reserve(matr_idx[2].size());
    std::vector<std::vector<IntT>>        new_int_v;  new_int_v .reserve(matr_idx[3].size());
    std::vector<std::vector<UIntT>>       new_uint_v; new_uint_v.reserve(matr_idx[4].size());
    std::vector<std::vector<FloatT>>      new_dbl_v;  new_dbl_v .reserve(matr_idx[5].size());

    for (auto& el : new_str_v)
        el.resize(new_nrow);
    for (auto& el : new_chr_v)
        el.resize(new_nrow);
    for (auto& el : new_bool_v)
        el.resize(new_nrow);
    for (auto& el : new_int_v)
        el.resize(new_nrow);
    for (auto& el : new_uint_v)
        el.resize(new_nrow);
    for (auto& el : new_dbl_v)
        el.resize(new_nrow);

    std::vector<std::vector<std::string>> new_tmp_val_refv(tmp_val_refv.size());

    auto compact_block_pod = [&]<typename T>(std::vector<T>& dst, 
                                             const std::vector<T>& src) {
        size_t i = 0;
        size_t written = 0;
        while (i < old_nrow) {
            const unsigned int ref_val = x[i];
            if (i < ref_val) {
                const size_t start = i;
                while (i < old_nrow && i < ref_val) ++i;
                const size_t len = i - start;
                std::memcpy(dst.data() + written, 
                            src.data() + start, 
                            len * sizeof(T));
                written += len;
            }
        }
    };

    auto compact_block_scalar = [&](auto& dst, 
                                    const auto& src) {
        size_t i = 0;
        while (i < old_nrow) {
            const unsigned int ref_val = x[i];
            if (i < ref_val) {
                while (i < old_nrow && i < ref_val) {
                    dst[i] = src[i];
                    i += 1;
                };
            }
        }
    };

    for (size_t t = 0; t < 6; ++t) {
        
        const auto& idx = matr_idx[t];
        const size_t ncols_t = idx.size();
        if (ncols_t == 0) continue;

        switch (t) {
            case 0: 
                for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                    compact_block_scalar(new_str_v[cpos], str_v[cpos]);
                break;
            case 1:
                for (size_t cpos = 0; cpos < ncols_t; ++cpos) {
                    compact_block_pod.template operator()<CharT>(new_chr_v[cpos],  
                                                                 chr_v[cpos]);
                }
                break;
            case 2: 
                for (size_t cpos = 0; cpos < ncols_t; ++cpos) {
                    compact_block_pod.template operator()<uint8_t>(new_bool_v[cpos],  
                                                                   bool_v[cpos]);
                }
                break;
            case 3:
                for (size_t cpos = 0; cpos < ncols_t; ++cpos) {
                    compact_block_pod.template operator()<IntT>(new_int_v[cpos], 
                                                                int_v[cpos]);
                }
                break;
            case 4: 
                for (size_t cpos = 0; cpos < ncols_t; ++cpos) {
                    compact_block_pod.template operator()<UIntT>(new_uint_v[cpos], 
                                                                 uint_v[cpos]);
                }
                break;
            case 5: 
                for (size_t cpos = 0; cpos < ncols_t; ++cpos) {
                    compact_block_pod.template operator()<FloatT>(new_dbl_v[cpos],
                                                                  dbl_v[cpos]);
                }
                break;
        }

    }

    for (size_t cpos = 0; cpos < ncol; ++cpos) {
        auto& src_aux = tmp_val_refv[cpos];
        auto& dst_aux = new_tmp_val_refv[cpos];
        dst_aux.reserve(new_nrow);
        size_t i = 0;
        while (i < old_nrow) {
            const unsigned int ref_val = x[i];
            if (i < ref_val) {
                while (i < old_nrow && i < ref_val) {
                    dst_aux[i] = std::move(src_aux[i]);
                    i += 1;
                };
            }
        }
    }

    std::vector<std::string> new_name_v_row;
    if (!name_v_row.empty()) {
        new_name_v_row.resize(new_nrow);
        size_t i = 0;
        while (i < old_nrow) {
            const unsigned int ref_val = x[i];
            if (i < ref_val) {
                while (i < old_nrow && i < ref_val) {
                    new_name_v_row[i] = std::move(name_v_row[i]);
                    i += 1;
                };
            }
        }
    }

    str_v.swap(new_str_v);
    chr_v.swap(new_chr_v);
    bool_v.swap(new_bool_v);
    int_v.swap(new_int_v);
    uint_v.swap(new_uint_v);
    dbl_v.swap(new_dbl_v);
    tmp_val_refv.swap(new_tmp_val_refv);
    name_v_row.swap(new_name_v_row);

    nrow = new_nrow;

}


