#pragma once

enum class MtMethod {
    Column,
    Row,
    Nested
};

template<MtMethod T>
struct is_supported_mt : std::false_type {};

template<>
struct is_supported_mt<MtMethod::Column> : std::true_type {};

template<>
struct is_supported_mt<MtMethod::Row> : std::true_type {};

template<>
struct is_supported_mt<MtMethod::Nested> : std::true_type {};

