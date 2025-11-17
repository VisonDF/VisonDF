#pragma once

template <bool ASC>
inline void sort_bool(
    std::vector<size_t>& idx,
    size_t n,
    size_t col_id)
{

    const size_t base = n * col_id;

    auto cmp = [&](size_t a, size_t b) {
        if constexpr (ASC)
            return bool_v[base + a] < bool_v[base + b];
        else
            return bool_v[base + a] > bool_v[base + b];
    };

    std::sort(idx.begin(), idx.end(), cmp);
}
