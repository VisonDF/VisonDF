#pragma once

template<typename U>
concept vector_or_span =
    std::same_as<std::remove_cvref_t<U>,
                 std::vector<typename std::remove_cvref_t<U>::value_type>> ||
    std::same_as<std::remove_cvref_t<U>,
                 std::span<typename std::remove_cvref_t<U>::element_type>>;
