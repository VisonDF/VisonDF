#pragma once

template <bool ASC> //, bool MT = true>
inline void sort_string(
    std::vector<size_t>& idx, 
    const std::string& values)
{
    auto cmp = [&](size_t a, size_t b) {
        if constexpr (ASC)
            return values[a] < values[b];
        else
            return values[a] > values[b];
    };

    //if constexpr (MT) {

        //std::stable_sort(idx.begin(), idx.end(), cmp);

    //} else if constexpr (!MT) {

        std::stable_sort(idx.begin(), idx.end(), cmp);

    //}

}
