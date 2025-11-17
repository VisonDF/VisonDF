#pragma once

template <bool ASC, 
          typename T, 
          typename ComparatorFactory = DefaultComparatorFactory>
inline void sort_string(
    std::vector<size_t>& idx, 
    const std::string& values,
    ComparatorFactory make_cmp = Cmp{})
{

    auto cmp = make_cmp.template operator()<ASC, std::string>(values);

    std::stable_sort(idx.begin(), idx.end(), cmp);

    if constexpr (!ASC) {
        std::reverse(idx.begin(), idx.end());
    }

}
