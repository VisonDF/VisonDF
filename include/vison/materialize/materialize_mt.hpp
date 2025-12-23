#pragma once

template <unsigned int CORES = 4, 
          bool IsDense = true,
          bool MemClean = false>
void materialize_mt() 
{
 
    if constexpr (IsDense) { // row_view_idx pased as const ref
        no_inplace_permutation<std::string, CORES>(str_v,  row_view_idx);
        no_inplace_permutation<CharT, CORES>      (chr_v,  row_view_idx);
        no_inplace_permutation<uint8_t, CORES>    (bool_v, row_view_idx);
        no_inplace_permutation<IntT, CORES>       (int_v,  row_view_idx);
        no_inplace_permutation<UIntT, CORES>      (uint_v, row_view_idx);
        no_inplace_permutation<FloatT, CORES>     (dbl_v,  row_view_idx);
    } else if constexpr (!IsDense) { // row_view_idx passed as deep copy
        inplace_permutation<std::string, CORES>(str_v,  row_view_idx);
        inplace_permutation<CharT, CORES>      (chr_v,  row_view_idx);
        inplace_permutation<uint8_t, CORES>    (bool_v, row_view_idx);
        inplace_permutation<IntT, CORES>       (int_v,  row_view_idx);
        inplace_permutation<UIntT, CORES>      (uint_v, row_view_idx);
        inplace_permutation<FloatT, CORES>     (dbl_v,  row_view_idx);
    }
    in_view = false;
    //row_view_idx.clear(); // unnecessary, may avoid unnecessary realloc
    row_view_map.clear();
    col_alrd_materialized.clear();

    if constexpr (MemClean) {
        for (auto& el : str_v) {
            el.resize(local_nrow);
            el.shrink_to_fit();
        }
        for (auto& el : chr_v) {
            el.resize(local_nrow);
            el.shrink_to_fit();
        }
        for (auto& el : bool_v) {
            el.resize(local_nrow);
            el.shrink_to_fit();
        }
        for (auto& el : int_v) {
            el.resize(local_nrow);
            el.shrink_to_fit();
        }
        for (auto& el : uint_v) {
            el.resize(local_nrow);
            el.shrink_to_fit();
        }
        for (auto& el : dbl_v) {
            el.resize(local_nrow);
            el.shrink_to_fit();
        }
    }

}


