#pragma once

template <bool ASC>
inline void radix_sort_uintegers(
    std::vector<size_t>& idx,
    unsigned int nrow,
    unsigned int col_id
)
{

    if constexpr (std::is_same_v<UIntT, uint8_t>) {

        const uint8_t* col = uint_v.data() + col_id * nrow;
        radix_sort_uint8(col, idx.data(), nrow);

    } else if constexpr (std::is_same_v<UIntT, uint16_t>) {

        const uint16_t* col = uint_v.data() + col_id * nrow;
        radix_sort_uint16(col, idx.data(), nrow);

    } else if constexpr (std::is_same_v<UIntT, uint32_t>) {

        const uint32_t* col = uint_v.data() + col_id * nrow;
        radix_sort_uint32(col, idx.data(), nrow);

    } else if constexpr (std::is_same_v<UIntT, uint64_t>) {

        const uint64_t* col = uint_v.data() + col_id * nrow;
        radix_sort_uint64(col, idx.data(), nrow);
        
    } else {
        static_assert(std::is_same_v<IntT, void>,
            "Unsupported IntT: must be uint8_t, uint16_t, uint32_t or uint64_t");
    }

    if constexpr (!ASC) {
      std::reverse(idx.begin(), idx.end());
    }

}

