#pragma once

template <bool MemClean = false>
void reinitiate() {
 
    nrow = 0;
    ncol = 0;

    sync_map_col {true, true, true, true, true, true};

    str_v                .clear();
    chr_v                .clear();
    bool_v               .clear();
    int_v                .clear();
    uint_v               .clear();
    dbl_v                .clear();
    name_v               .clear();
    name_v_row           .clear();
    longest_v            .clear();
    row_view_idx         .clear(); 
    type_refv            .clear();
    tmp_val_refv         .clear();
    col_alrd_materialized.clear();

    for (auto& el : matr_idx)
        el.clear();

    for (auto& el : matr_idx_map)
        el.clear();

    if constexpr (MemClean) {

        str_v                .shrink_to_fit();
        chr_v                .shrink_to_fit();
        bool_v               .shrink_to_fit();
        int_v                .shrink_to_fit();
        uint_v               .shrink_to_fit();
        dbl_v                .shrink_to_fit();
        name_v               .shrink_to_fit();
        name_v_row           .shrink_to_fit();
        longest_v            .shrink_to_fit();
        row_view_idx         .shrink_to_fit(); 
        type_refv            .shrink_to_fit();
        tmp_val_refv         .shrink_to_fit();
        col_alrd_materialized.shrink_to_fit();

        for (auto& el : matr_idx)
            el.shrink_to_fit();

        for (auto& el : matr_idx_map)
            el.shrink_to_fit();

    }

};





