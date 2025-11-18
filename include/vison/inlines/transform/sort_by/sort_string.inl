#pragma once

template <bool ASC, 
          unsigned int CORES = 4,
          bool Simd = true,
          SortType S = SortType::Radix,
          typename ComparatorFactory = DefaultComparatorFactory>
inline void sort_string(
    std::vector<size_t>& idx, 
    unsigned int nrow,
    unsigned int col_id,
    ComparatorFactory make_cmp = ComparatorFactory{})
{

    const std::string* col = str_v.data() + col_id * nrow;

    auto cmp = make_cmp.template operator()<ASC, std::string>(col);

    if constexpr (S == SortType::Radix) {

            return;

    } else if constexpr (S == SortType::Standard) {

            if constexpr (CORES == 1) {
    
            std::stable_sort(idx.begin(), idx.end(), cmp);

            } else if constexpr (CORES > 1) {

                return;

            }

    }

    if constexpr (!ASC) {
        std::reverse(idx.begin(), idx.end());
    }

}



