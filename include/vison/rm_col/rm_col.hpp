#pragma once

template <bool MemClean = false>
void rm_col(unsigned int& nbcol) {

    std::vector<size_t> col_type(ncol);
    for (size_t type_i = 0; type_i < 6; ++type_i)
        for (auto col : matr_idx[type_i])
            col_type[col] = type_i;

    size_t type_i = col_type[nbcol];
    size_t idx_in_type =
        std::find(matr_idx[type_i].begin(), matr_idx[type_i].end(), nbcol)
        - matr_idx[type_i].begin();

    if (!name_v.empty()) { name_v.erase(name_v.begin() + nbcol); }
    tmp_val_refv.erase(tmp_val_refv.begin() + nbcol);
    type_refv.erase(type_refv.begin() + nbcol);
    matr_idx[type_i].erase(matr_idx[type_i].begin() + idx_in_type);

    if constexpr (MemClean) {

        switch (type_i) {
            case 0: str_v. erase(str_v.begin()  + idx_in_type); str_v.shrink_to_fit();  break;
            case 1: chr_v. erase(chr_v.begin()  + idx_in_type); chr_v.shrink_to_fit();  break;
            case 2: bool_v.erase(bool_v.begin() + idx_in_type); bool_v.shrink_to_fit(); break;
            case 3: int_v. erase(int_v.begin()  + idx_in_type); int_v.shrink_to_fit();  break;
            case 4: uint_v.erase(uint_v.begin() + idx_in_type); uint_v.shrink_to_fit(); break;
            case 5: dbl_v. erase(dbl_v.begin()  + idx_in_type); dbl_v.shrink_to_fit();  break;
        }

        name_v.shrink_to_fit();
        tmp_val_refv.shrink_to_fit();
        type_refv.shrink_to_fit();
        matr_idx[type_i].shrink_to_fit();

    } else if constexpr (!MemClean) {

        switch (type_i) {
            case 0: str_v. erase(str_v.begin()  + idx_in_type);  break;
            case 1: chr_v. erase(chr_v.begin()  + idx_in_type);  break;
            case 2: bool_v.erase(bool_v.begin() + idx_in_type);  break;
            case 3: int_v. erase(int_v.begin()  + idx_in_type);  break;
            case 4: uint_v.erase(uint_v.begin() + idx_in_type);  break;
            case 5: dbl_v. erase(dbl_v.begin()  + idx_in_type);  break;
        }

    }

    --ncol;
}


