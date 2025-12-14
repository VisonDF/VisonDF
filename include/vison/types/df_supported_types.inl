#pragma once

template <typename T>
struct df_supported_type : std::false_type {};

template <> struct df_supported_type<std::string> : std::true_type {};
template <> struct df_supported_type<CharT>       : std::true_type {};
template <> struct df_supported_type<uint8_t>     : std::true_type {};
template <> struct df_supported_type<IntT>        : std::true_type {};
template <> struct df_supported_type<UIntT>       : std::true_type {};
template <> struct df_supported_type<FloatT>      : std::true_type {};

template <typename T>
concept DfSupportedType = is_supported_type<std::decay_t<T>>::value;
