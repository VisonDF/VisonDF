#pragma once

template <bool ASC, class SpanT>
inline void sort_idx_using_span(
    std::vector<size_t>& idx, 
    const SpanT& values)
{
    auto cmp = [&](size_t a, size_t b) {
        if constexpr (ASC)
            return values[a] < values[b];
        else
            return values[a] > values[b];
    };

    std::sort(idx.begin(), idx.end(), cmp);
}
