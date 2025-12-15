#pragma once

template <typename T, typename = void>
struct element_t {
    using type = T;
};

template <typename T>
struct element_t<T, std::void_t<typename std::remove_cvref_t<T>::value_type>> {
    using type = typename element_t<
                                       std::remove_cvref_t<T>::value_type
                                   >::type;
};

template <typename T>
using element_type_t = element_t<T>::type;



