struct DefaultComparatorFactory {
    template<bool ASC, typename T>
    auto operator()(const T* col) const {
        return [col](size_t a, size_t b) {
            if constexpr (ASC)
                return col[a] < col[b];
            else
                return col[a] > col[b];
        };
    }
};
