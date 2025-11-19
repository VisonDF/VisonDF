#pragma once

template <typename T>
struct array_length;

template <typename T, std::size_t N>
struct array_length<T[N]> {
    static constexpr std::size_t value = N;
};
