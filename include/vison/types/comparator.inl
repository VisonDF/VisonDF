template<typename Cmp>
concept Comparator =
    std::predicate<Cmp, size_t, size_t>;
