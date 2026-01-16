template<typename T>
struct is_supported_bool 
    : std::bool_constant<
          std::is_same_v<T, uint8_t>
      > {};

template<typename T>
struct is_supported_int 
    : std::bool_constant<
          std::is_same_v<T, int8_t>  ||
          std::is_same_v<T, int16_t> ||
          std::is_same_v<T, int32_t> ||
          std::is_same_v<T, int64_t>
      > {};

template<typename T>
struct is_supported_uint 
    : std::bool_constant<
          std::is_same_v<T, uint8_t>  ||
          std::is_same_v<T, uint16_t> ||
          std::is_same_v<T, uint32_t> ||
          std::is_same_v<T, uint64_t>
      > {};

template<typename T>
struct is_supported_decimal 
    : std::bool_constant<
          std::is_same_v<T, float> ||
          std::is_same_v<T, double>
      > {};

template<typename T>
struct is_supported_type
    : std::bool_constant<
          std::is_same_v<T, std::string> ||
          std::is_same_v<T, CharT>       ||
          std::is_same_v<T, uint8_t>     ||
          std::is_same_v<T, IntT>        ||
          std::is_same_v<T, UIntT>       ||
          std::is_same_v<T, FloatT>      ||
          std::is_same_v<T, void>
      > {};




