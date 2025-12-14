#pragma once

inline value_t make_vec(unsigned int x, unsigned int n)
{
    switch (x) {
        case 0: return value_t(std::in_place_type<ReservingVec<std::string>>,     n);
        case 1: return value_t(std::in_place_type<ReservingVec<CharT>>,           n);
        case 2: return value_t(std::in_place_type<ReservingVec<uint8_t>>,         n);
        case 3: return value_t(std::in_place_type<ReservingVec<IntT>>,            n);
        case 4: return value_t(std::in_place_type<ReservingVec<UIntT>>,           n);
        case 5: return value_t(std::in_place_type<ReservingVec<FloatT>>,          n);
        default:
            std::abort();
    }
}
