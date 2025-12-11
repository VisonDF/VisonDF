enum class GroupFunction {
    Occurence,
    Sum,
    Mean,
    Gather
};

template<GroupFunction T>
struct is_supported_group_function : std::false_type {};

template<>
struct is_supported_group_function<GroupFunction::Occurence> : std::true_type {};

template<>
struct is_supported_group_function<GroupFunction::Sum> : std::true_type {};

template<>
struct is_supported_group_function<GroupFunction::Mean> : std::true_type {};

template<>
struct is_supported_group_function<GroupFunction::Gather> : std::true_type {};

