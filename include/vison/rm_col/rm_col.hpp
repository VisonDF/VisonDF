#pragma once

template <typename T = void,
           bool MapCol = false,
           bool MemClean = false>
void rm_col(unsigned int& nbcol) {

    size_t idx_in_type;
    size_t idx_type;

    if constexpr (std::is_same_v<T, void>) {

        std::vector<size_t> col_type(ncol);
        for (size_t type_i = 0; type_i < 6; ++type_i)
            for (auto col : matr_idx[type_i])
                col_type[col] = type_i;

        idx_type = col_type[nbcol];

    } else {

        if constexpr (std::is_same_v<element_type_t<T>, std::string>) {
            idx_type = 0;
        } else if constexpr (std::is_same_v<element_type_t<T>, CharT>) {
            idx_type = 1;
        } else if constexpr (std::is_same_v<element_type_t<T>, uint8_t>) {
            idx_type = 2;
        } else if constexpr (std::is_same_v<element_type_t<T>, IntT>) {
            idx_type = 3;
        } else if constexpr (std::is_same_v<element_type_t<T>, UIntT>) {
            idx_type = 4;
        } else if constexpr (std::is_same_v<element_type_t<T>, FloatT>) {
            idx_type = 5;
        } else {
            std::cerr << "Impossible type\n";
            return;
        }

    }

    if constexpr (!MapCol) {

        idx_in_type =
            std::find(matr_idx[idx_type].begin(), matr_idx[idx_type].end(), nbcol)
            - matr_idx[idx_type].begin();

    } else {

        if (!matr_idx_map[idx_type].contains(nbcol)) {
            std::cerr << "MapCol chosen, but no corresponding col found in the map\n";
            return;
        }

        if (!sync_map_col[idx_type]) {
            std::cerr << "MapCol not synced\n";
            return;
        }

        idx_type = matr_idx_map[idx_type][nbcol];

    }

    if (!name_v.empty()) { name_v.erase(name_v.begin() + nbcol); }
    type_refv.erase(type_refv.begin() + nbcol);
    matr_idx[idx_type].erase(matr_idx[idx_type].begin() + idx_in_type);
    matr_idx_map[idx_type].erase(nbcol);

    switch (type_i) {
        case 0: {
                    str_v.erase(str_v.begin()  + idx_in_type); 
                    sync_map_col[0] = false; 
                    if constexpr (MemClean) str_v.shrink_to_fit(); 
                    break;
                }
        case 1: {
                    chr_v.erase(chr_v.begin()  + idx_in_type); 
                    sync_map_col[1] = false;
                    if constexpr (MemClean) chr_v.shrink_to_fit(); 
                    break;
                }
        case 2: {
                    bool_v.erase(bool_v.begin() + idx_in_type); 
                    sync_map_col[2] = false;
                    if constexpr (MemClean) bool_v.shrink_to_fit(); 
                    break;
                }
        case 3: {
                    int_v.erase(int_v.begin()  + idx_in_type); 
                    sync_map_col[3] = false;
                    if constexpr (MemClean) int_v.shrink_to_fit(); 
                    break;
                }
        case 4: {
                    uint_v.erase(uint_v.begin() + idx_in_type); 
                    sync_map_col[4] = false;
                    if constexpr (MemClean) uint_v.shrink_to_fit(); 
                    break;
                }
        case 5: {
                    dbl_v.erase(dbl_v.begin()  + idx_in_type); 
                    sync_map_col[5] = false; 
                    if constexpr (MemClean) dbl_v.shrink_to_fit(); 
                    break;
                }
    }

    if constexpr (MemClean) {
        name_v.shrink_to_fit();
        type_refv.shrink_to_fit();
        matr_idx[idx_type].shrink_to_fit();
    }

    --ncol;
}






