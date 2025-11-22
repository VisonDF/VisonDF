#pragma once

enum class StringRadixType {
    Padding,
    FlatPadding,
    NoPadding
};

template<StringRadixType T>
struct is_supported_string_radix : std::false_type {};

template<>
struct is_supported_string_radix<StringRadixType::Padding> : std::true_type {};

template<>
struct is_supported_string_radix<StringRadixType::FlatPadding> : std::true_type {};

template<>
struct is_supported_string_radix<StringRadixType::NoPadding> : std::true_type {};

