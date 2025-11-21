#pragma once

bool operator==(const Dataframe& o) const {

    const bool nrow_cmp = nrow == o.nrow;
    const bool ncol_cmp = ncol == o.ncol;

    const bool str_v_cmp  = str_v  == o.str_v ;
    const bool chr_v_cmp  = chr_v  == o.chr_v ;
    const bool bool_v_cmp = bool_v == o.bool_v;
    const bool int_v_cmp  = int_v  == o.int_v ;
    const bool uint_v_cmp = uint_v == o.uint_v;
    const bool dbl_v_cmp  = dbl_v  == o.dbl_v ;

    const bool matr_idx_cmp   = matr_idx   == o.matr_idx  ;
    const bool name_v_cmp     = name_v     == o.name_v    ;
    const bool name_v_row_cmp = name_v_row == o.name_v_row;
    const bool longest_v_cmp  = longest_v  == o.longest_v ;

    const bool type_refv_cmp    = type_refv    == o.type_refv   ;
    const bool tmp_val_refv_cmp = tmp_val_refv == o.tmp_val_refv;

    return (nrow_cmp          &&
            ncol_cmp          &&
            str_v_cmp         &&
            chr_v_cmp         &&
            bool_v_cmp        &&
            int_v_cmp         &&
            uint_v_cmp        &&
            dbl_v_cmp         &&
            matr_idx_cmp      &&
            name_v_cmp        &&
            name_v_row_cmp    &&
            longest_v_cmp     &&
            type_refv_cmp     &&
            tmp_val_refv_cmp);

}

bool operator!=(const Dataframe& o) const {
    return !(*this == o);
}



