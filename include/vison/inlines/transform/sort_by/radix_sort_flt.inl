#pragma once

template <bool ASC, unsigned int CORES = 4, bool Simd = true>
inline void radix_sort_flt(
    std::vector<size_t>& idx,
    unsigned int nrow,
    unsigned int col_id
)
{
    if constexpr (std::is_same_v<FloatT, float>) {

        const float* col = int_v.data() + col_id * nrow;

        if constexpr (CORES == 1) {

            radix_sort_float<Simd>(col, idx.data(), nrow);

        } else if constexpr (CORES > 1) {

            radix_sort_float_mt<CORES, Simd>(col, idx.data(), nrow);

        }

    } else if constexpr (std::is_same_v<FloatT, double>) {

        const double* col = int_v.data() + col_id * nrow;

        if constexpr (CORES == 1) {

            radix_sort_double<Simd>(col, idx.data(), nrow);

        } else if constexpr (CORES > 1) {

            radix_sort_double_mt<CORES, Simd>(col, idx.data(), nrow);

        }
        
    } else {
        static_assert(std::is_same_v<FloatT, void>,
            "Unsupported IntT: must be int32_t or int64_t");
    }

    if constexpr (!ASC) {
      std::reverse(idx.begin(), idx.end());
    }

}


