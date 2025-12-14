#pragma once

inline value_t make_zero(unsigned int x)
{
    switch (x) {
        case 2: return value_t(std::in_place_type<uint8_t>, 0);
        case 3: return value_t(std::in_place_type<IntT>,    0);
        case 4: return value_t(std::in_place_type<UIntT>,   0);
        case 5: return value_t(std::in_place_type<FloatT>,  0);
        default:
            std::abort();
    }
}
