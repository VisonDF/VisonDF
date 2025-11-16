#pragma once

template <bool ASC>
inline void sort_bool(
    std::vector<size_t>& idx,
    const std::vector<bool>& col,
    size_t base)
{
    auto cmp = [&](size_t a, size_t b) {
        if constexpr (ASC)
            return col[base + a] < col[base + b];
        else
            return col[base + a] > col[base + b];
    };

    std::sort(idx.begin(), idx.end(), cmp);
}
