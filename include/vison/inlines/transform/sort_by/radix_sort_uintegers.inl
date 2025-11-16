#pragma once

template <bool ASC, unsigned int CORES = 1, bool Simd = true>
inline void radix_sort_uintegers(
    std::vector<size_t>& idx,
    unsigned int nrow,
    unsigned int col_id
)
{

    if constexpr (std::is_same_v<UIntT, uint8_t>) {

        const uint8_t* col = uint_v.data() + col_id * nrow;

        if constexpr (CORES == 1) {

            radix_sort_uint8<Simd>(col, idx.data(), nrow);

        } else if constexpr (CORES > 1) {

            radix_sort_uint8_mt<CORES, Simd>(col, idx.data(), nrow);

        }

    } else if constexpr (std::is_same_v<UIntT, uint16_t>) {

        const uint16_t* col = int_v.data() + col_id * nrow;

        if constexpr (CORES == 1) {

            radix_sort_uint16<Simd>(col, idx.data(), nrow);

        } else if constexpr (CORES > 1) {

            radix_sort_uint16_mt<CORES, Simd>(col, idx.data(), nrow);

        }

    } else if constexpr (std::is_same_v<UIntT, uint32_t>) {

        const uint32_t* col = int_v.data() + col_id * nrow;

        if constexpr (CORES == 1) {

            radix_sort_uint32<Simd>(col, idx.data(), nrow);

        } else if constexpr (CORES > 1) {

            radix_sort_uint32_mt<CORES, Simd>(col, idx.data(), nrow);

        }

    } else if constexpr (std::is_same_v<UIntT, uint64_t>) {

        const uint64_t* col = int_v.data() + col_id * nrow;

        if constexpr (CORES == 1) {

            radix_sort_uint64<Simd>(col, idx.data(), nrow);

        } else if constexpr (CORES > 1) {

            radix_sort_uint64_mt<CORES, Simd>(col, idx.data(), nrow);

        }
        
    } else {
        static_assert(std::is_same_v<UIntT, void>,
            "Unsupported IntT: must be int32_t or int64_t");
    }

    if constexpr (!ASC) {
      std::reverse(idx.begin(), idx.end());
    }

}

