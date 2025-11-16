#pragma once

template <bool ASC, unsigned int CORES = 4, bool Simd = true>
inline void radix_sort_integers(
    std::vector<size_t>& idx,
    unsigned int nrow,
    unsigned int col_id
)
{

    if constexpr (std::is_same_v<IntT, int8_t>) {

        const int8_t* col = int_v.data() + col_id * nrow;

        if constexpr (CORES == 1) {

            radix_sort_int8<Simd>(col, idx.data(), nrow);

        } else if constexpr (CORES > 1) {

            radix_sort_int8_mt<CORES, Simd>(col, idx.data(), nrow);

        }

    } else if constexpr (std::is_same_v<IntT, int16_t>) {

        const int16_t* col = int_v.data() + col_id * nrow;

        if constexpr (CORES == 1) {

            radix_sort_int16<Simd>(col, idx.data(), nrow);

        } else if constexpr (CORES > 1) {

            radix_sort_int16_mt<CORES, Simd>(col, idx.data(), nrow);

        }

    } else if constexpr (std::is_same_v<IntT, int32_t>) {

        const int32_t* col = int_v.data() + col_id * nrow;

        if constexpr (CORES == 1) {

            radix_sort_int32<Simd>(col, idx.data(), nrow);

        } else if constexpr (CORES > 1) {

            radix_sort_int32_mt<CORES, Simd>(col, idx.data(), nrow);

        }

    } else if constexpr (std::is_same_v<IntT, int64_t>) {

        const int64_t* col = int_v.data() + col_id * nrow;

        if constexpr (CORES == 1) {

            radix_sort_int64<Simd>(col, idx.data(), nrow);

        } else if constexpr (CORES > 1) {

            radix_sort_int64_mt<CORES, Simd>(col, idx.data(), nrow);

        }
        
    } else {
        static_assert(std::is_same_v<IntT, void>,
            "Unsupported IntT: must be int32_t or int64_t");
    }

    if constexpr (!ASC) {
      std::reverse(idx.begin(), idx.end());
    }

}


