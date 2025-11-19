#pragma once

template <bool MemClean = false>
void rm_col_range_reconstruct(std::vector<unsigned int> rm_cols) {
    if (rm_cols.empty()) return;

    const size_t old_ncol = ncol;
    const size_t rows     = nrow;

    std::sort(rm_cols.begin(), rm_cols.end());
    std::vector<unsigned int> all(old_ncol);
    std::iota(all.begin(), all.end(), 0);

    std::vector<unsigned int> keep;
    keep.reserve(old_ncol);
    std::set_difference(all.begin(), all.end(),
                        rm_cols.begin(), rm_cols.end(),
                        std::back_inserter(keep));

    std::vector<size_t> col_type(old_ncol);
    std::vector<size_t> col_pos_in_type(old_ncol);
    for (size_t t = 0; t < matr_idx.size(); ++t) {
        for (size_t pos = 0; pos < matr_idx[t].size(); ++pos) {
            unsigned g = matr_idx[t][pos];       
            col_type[g] = t;
            col_pos_in_type[g] = pos;            
        }
    }

    std::array<size_t, 6> kept_per_type{}; 
    for (unsigned g : keep) kept_per_type[col_type[g]]++;

    std::vector<std::string> new_str_v;  new_str_v .reserve(kept_per_type[0] * rows);
    std::vector<CharT>       new_chr_v;  new_chr_v .reserve(kept_per_type[1] * rows);
    std::vector<uint8_t>     new_bool_v; new_bool_v.reserve(kept_per_type[2] * rows);
    std::vector<IntT>        new_int_v;  new_int_v .reserve(kept_per_type[3] * rows);
    std::vector<UIntT>       new_uint_v; new_uint_v.reserve(kept_per_type[4] * rows);
    std::vector<FloatT>      new_dbl_v;  new_dbl_v .reserve(kept_per_type[5] * rows);

    std::vector<std::vector<unsigned>> new_matr_idx;
    std::vector<char>                     new_type_refv;   new_type_refv  .reserve(keep.size());
    std::vector<std::vector<std::string>> new_tmp_val_refv;new_tmp_val_refv.reserve(keep.size());
    std::vector<std::string> new_name_v;      
    if (!name_v.empty()) {
      new_name_v.reserve(keep.size());
    }

    for (size_t new_g = 0; new_g < keep.size(); ++new_g) {
        unsigned old_g = keep[new_g];
        size_t t       = col_type[old_g];
        size_t pos_old = col_pos_in_type[old_g];   
        size_t base    = pos_old * rows;

        new_name_v     .push_back(std::move(name_v[old_g]));
        new_type_refv  .push_back(type_refv[old_g]);
        new_tmp_val_refv.push_back(std::move(tmp_val_refv[old_g]));
        new_matr_idx[t].push_back(static_cast<unsigned>(new_g)); 

        switch (t) {
            case 0: {
                     new_str_v.insert(new_str_v.end(),
                     std::make_move_iterator(str_v.begin() + base),
                     std::make_move_iterator(str_v.begin() + base + rows));
                     break;
                     }
            case 1: {
                        new_chr_v.resize(new_chr_v.size() + rows);
                        std::memcpy(new_chr_v.data() + new_chr_v.size() - rows,
                                    chr_v.data() + base,
                                    rows * sizeof(CharT));
                        break;
                    }
            case 2:
                new_bool_v.insert(new_bool_v.end(), 
                                bool_v.begin() + base, bool_v.begin() + base + rows); break;
            case 3: {
                        new_int_v.resize(new_int_v.size() + rows);
                        std::memcpy(new_int_v.data() + new_int_v.size() - rows,
                                    int_v.data() + base,
                                    rows * sizeof(IntT));
                        break;
                    }
            case 4: {
                        new_uint_v.resize(new_uint_v.size() + rows);
                        std::memcpy(new_uint_v.data() + new_uint_v.size() - rows,
                                    uint_v.data() + base,
                                    rows * sizeof(UIntT));
                        break;
                    }
            case 5: {
                        new_dbl_v.resize(new_dbl_v.size() + rows);
                        std::memcpy(new_dbl_v.data() + new_dbl_v.size() - rows,
                                    dbl_v.data() + base,
                                    rows * sizeof(FloatT));
                        break;

                    }
        }
    }

    name_v.swap(new_name_v);
    type_refv.swap(new_type_refv);
    tmp_val_refv.swap(new_tmp_val_refv);
    matr_idx.swap(new_matr_idx);

    str_v.swap(new_str_v);
    chr_v.swap(new_chr_v);
    bool_v.swap(new_bool_v);
    int_v.swap(new_int_v);
    uint_v.swap(new_uint_v);
    dbl_v.swap(new_dbl_v);

    ncol = keep.size();

    if constexpr (MemClean) {
        name_v.shrink_to_fit();
        type_refv.shrink_to_fit();
        tmp_val_refv.shrink_to_fit();
        for (auto& v : tmp_val_refv) v.shrink_to_fit();
        for (auto& v : matr_idx) v.shrink_to_fit();
        str_v.shrink_to_fit(); chr_v.shrink_to_fit(); bool_v.shrink_to_fit();
        int_v.shrink_to_fit(); uint_v.shrink_to_fit(); dbl_v.shrink_to_fit();
    }
}



