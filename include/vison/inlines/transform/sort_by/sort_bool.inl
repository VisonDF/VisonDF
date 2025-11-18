#pragma once

template <bool ASC, 
          unsigned int CORES = 1,
          bool Simd = true,
          SortType S = SortType::Radix,
          typename ComparatorFactory = DefaultComparatorFactory>
inline void sort_bool(
    std::vector<size_t>& idx,
    size_t n,
    size_t col_id,
    ComparatorFactory make_cmp = ComparatorFactory{}
    )
{ 

    const BoolT* col = bool_v.data() + col_id * nrow;

    auto cmp = make_cmp.template operator()<ASC, UIntT>(col);
    static_assert(IndexComparator<decltype(cmp)>,
              "Comparator must be cmp(size_t,size_t)->bool");

    if constexpr (S == SortType::Radix) {

        static_assert(std::is_same_v<BoolT, uint8_t> && ,
              "Comparator must be cmp(size_t,size_t)->bool");

    } else if constexpr (S == SorType::Standard) {

        if constexpr (CORES == 1) {

                std::sort(idx.begin(), idx.end(), cmp);

        } else if constexpr (CORES > 1) {

                return;

        }

    }

    if constexpr (!ASC) {
        std:reverse(idx.begin(), idx.end());
    }

}



