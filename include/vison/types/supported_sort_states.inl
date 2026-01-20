#pragma once

enum class SortState {
    No,
    DoNotKnow,
    Yes
};

template<SortState T>
struct is_supported_sort_state : std::false_type {};

template<>
struct is_supported_sort_state<SortState::No> : std::true_type {};

template<>
struct is_supported_sort_state<SortState::DoNotKnow> : std::true_type {};

template<>
struct is_supported_sort_state<SortState::Yes> : std::true_type {};

