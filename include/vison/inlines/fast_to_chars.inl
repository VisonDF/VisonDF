#pragma once

template <typename T>
inline std::pair<char*, std::errc>
fast_to_chars(char* first, char* last, T value) noexcept {
    if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
        char* end = jkj::dragonbox::to_chars(value, first);
        return {end, std::errc{}};
    }
    else if constexpr (std::is_integral_v<T>) {
        return std::to_chars(first, last, value);
    }
    else {
        static_assert(std::is_arithmetic_v<T>, 
                      "fast_to_chars only supports arithmetic types");
    }
}
