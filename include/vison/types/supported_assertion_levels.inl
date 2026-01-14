#pragma once

enum class AssertionLevel {
    Simple,
    Hard
};

template<AssertionLevel T>
struct is_supported_assertion_level : std::false_type {};

template<>
struct is_supported_assertion_level<AssertionLevel::Simple> : std::true_type {};

template<>
struct is_supported_assertion_level<AssertionLevel::Hard> :   std::true_type {};


