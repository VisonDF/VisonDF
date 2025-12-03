enum class LeftJoinMethods {
    First,
    Last,
    Aligned
};

template<LeftJoinMethods T>
struct is_supported_leftjoin : std::false_type {};

template<>
struct is_supported_leftjoin<LeftJoinMethods::First> : std::true_type {};

template<>
struct is_supported_leftjoin<LeftJoinMethods::Last> : std::true_type {};

template<>
struct is_supported_leftjoin<LeftJoinMethods::Aligned> : std::true_type {};

