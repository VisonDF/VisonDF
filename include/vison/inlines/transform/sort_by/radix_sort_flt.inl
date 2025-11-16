#pragma once

template <bool ASC>
inline void radix_sort_flt(
    std::vector<size_t>& idx,
    unsigned int nrow,
    unsigned int col_id
)
{
    if constexpr (std::is_same_v<FloatT, float>) {

        const float* col = int_v.data() + col_id * nrow;
        radix_sort_float(col, idx.data(), nrow);

    } else if constexpr (std::is_same_v<FloatT, double>) {

        const double* col = int_v.data() + col_id * nrow;
        radix_sort_double(col, idx.data(), nrow);
        
    } else {
        static_assert(std::is_same_v<IntT, void>,
            "Unsupported IntT: must be int32_t or int64_t");
    }

    if constexpr (!ASC) {
      std::reverse(idx.begin(), idx.end());
    }

}


