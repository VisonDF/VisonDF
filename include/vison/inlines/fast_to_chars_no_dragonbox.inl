#pragma once

template <typename T>
inline std::pair<char*, std::errc>
fast_to_chars(char* first, char* last, T value) noexcept {
    return std::to_chars(first, last, value);
}
