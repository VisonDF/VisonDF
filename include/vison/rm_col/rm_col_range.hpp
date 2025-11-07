#pragma once

void rm_col_range(std::vector<unsigned int>& nbcolv) {
    if (nbcolv.empty()) return;

    std::sort(nbcolv.begin(), nbcolv.end());
    std::reverse(nbcolv.begin(), nbcolv.end());

    std::vector<size_t> col_type(ncol);
    for (size_t type_i = 0; type_i < matr_idx.size(); ++type_i)
        for (auto col : matr_idx[type_i])
            col_type[col] = type_i;

    auto erase_block = [&](auto& vec, size_t off){
        vec.erase(vec.begin() + off, vec.begin() + off + nrow);
    };

    for (unsigned nbcol : nbcolv) {
        if (nbcol >= ncol) {
            std::cerr << "The column does not exist\n";
            continue;
        }

        size_t type_i = col_type[nbcol];
        size_t idx_in_type =
            std::find(matr_idx[type_i].begin(), matr_idx[type_i].end(), nbcol)
            - matr_idx[type_i].begin();

        name_v.erase(name_v.begin() + nbcol);
        tmp_val_refv.erase(tmp_val_refv.begin() + nbcol);
        type_refv.erase(type_refv.begin() + nbcol);
        matr_idx[type_i].erase(matr_idx[type_i].begin() + idx_in_type);

        size_t offset = idx_in_type * nrow;
        switch (type_i) {
            case 0: erase_block(str_v, offset); break;
            case 1: erase_block(chr_v, offset); break;
            case 2: erase_block(bool_v, offset); break;
            case 3: erase_block(int_v, offset); break;
            case 4: erase_block(uint_v, offset); break;
            case 5: erase_block(dbl_v, offset); break;
        }

        --ncol;
    }
}


