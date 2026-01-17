#pragma once

template <typename T>
inline constexpr size_t max_chars_needed() noexcept {
    return std::numeric_limits<T>::digits10 + 3;
} 
