#pragma once

template <bool ASC, 
          typename T, 
          Comparator Cmp = std::less<size_t>>
inline void sort_string(
    std::vector<size_t>& idx, 
    const std::string& values,
    Cmp cmp = Cmp{})
{

    std::stable_sort(idx.begin(), idx.end(), cmp);

    if constexpr (!ASC) {
        std::reverse(idx.begin(), idx.end());
    }

}
