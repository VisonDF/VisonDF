enum class SortType {
    Standard,
    Radix
};

template<SortType T>
struct is_supported_sort : std::false_type {};

template<>
struct is_supported_sort<SortType::Standard> : std::true_type {};

template<>
struct is_supported_sort<SortType::Radix> : std::true_type {};


