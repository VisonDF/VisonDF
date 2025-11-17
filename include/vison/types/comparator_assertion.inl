#pragma once

template<typename Cmp>
concept IndexComparator =
    requires(Cmp cmp, size_t a, size_t b)
    {
        { cmp(a, b) } -> std::convertible_to<bool>;
    };
