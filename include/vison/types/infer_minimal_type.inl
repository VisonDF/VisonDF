#pragma once

template <typename T>
struct strip_vector {
    using type = T;
};

template <typename U, typename Alloc>
struct strip_vector<std::vector<U, Alloc>> {
    using type = U;
};

template <typename T>
using strip_vector_t = typename strip_vector<T>::type;
