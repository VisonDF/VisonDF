#pragma once

template <bool IsDense = true>
void materialize() 
{

    // <type / CORES>
    if constexpr (IsDense) { // row_view_idx pased as const ref
        no_inplace_permutation<std::string, 1>(str_v,  row_view_idx);
        no_inplace_permutation<CharT, 1>      (chr_v,  row_view_idx);
        no_inplace_permutation<uint8_t, 1>    (bool_v, row_view_idx);
        no_inplace_permutation<IntT, 1>       (int_v,  row_view_idx);
        no_inplace_permutation<UIntT, 1>      (uint_v, row_view_idx);
        no_inplace_permutation<FloatT, 1>     (dbl_v,  row_view_idx);
    } else if constexpr (!IsDense) { // row_view_idx passed as deep copy
        inplace_permutation<std::string, 1>(str_v,  row_view_idx);
        inplace_permutation<CharT, 1>      (chr_v,  row_view_idx);
        inplace_permutation<uint8_t, 1>    (bool_v, row_view_idx);
        inplace_permutation<IntT, 1>       (int_v,  row_view_idx);
        inplace_permutation<UIntT, 1>      (uint_v, row_view_idx);
        inplace_permutation<FloatT, 1>     (dbl_v,  row_view_idx);
    }
    in_view = false;
    row_view_map.clear();
}


