#pragma once

template <bool ASC>
inline void sort_idx_using_span_uintegers(
    std::vector<size_t>& idx,
    unsigned int nrow,
    unsigned int col_id
)
{
    if constexpr (std::is_same_v<IntT, uint32_t>) {

        const int32_t* col = int_v.data() + col_id * nrow;
        radix_sort_uint32(col, idx.data(), nrow);

    } else if constexpr (std::is_same_v<IntT, uint64_t>) {

        const int64_t* col = int_v.data() + col_id * nrow;
        radix_sort_uint64(col, idx.data(), nrow);
        
    } else {
        static_assert(std::is_same_v<IntT, void>,
            "Unsupported IntT: must be int32_t or int64_t");
    }
    if constexpr (!ASC) {
      std::reverse(idx.begin(), idx.end());
    }

}

