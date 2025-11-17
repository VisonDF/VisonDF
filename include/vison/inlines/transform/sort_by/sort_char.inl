#pragma once

template <bool ASC, unsigned int CORES = 4, bool Simd = true>
inline void radix_sort_char(
    std::vector<size_t>& idx,
    unsigned int nrow,
    unsigned int col_id
)
{

    const int8_t* col = chr_v.data() + col_id * nrow;

    if constexpr (CORES == 1) {

        radix_sort_int8<Simd>(col, idx.data(), nrow);

    } else if constexpr (CORES > 1) {

        radix_sort_int8_mt<CORES, Simd>(col, idx.data(), nrow);

    }

    if constexpr (!ASC) {
      std::reverse(idx.begin(), idx.end());
    }

}


