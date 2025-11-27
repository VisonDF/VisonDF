#pragma once

struct GetDataframeFilterIdxSimd {
    const std::vector<size_t>*       cols;
    const std::vector<unsigned int>* mask;

    std::shared_ptr<std::vector<size_t>>       owned_cols;
    std::shared_ptr<std::vector<unsigned int>> owned_mask;

    template<typename DF>
    auto operator()(const DF& df) const {
        DF out;
        df.get_dataframe_filter_idx_simd(*cols, out, *mask);
        return out;
    }
};

inline GetDataframeFilterIdxSimd get_dataframe_filter_idx_simd(
                                                      const std::vector<size_t>& cols,
                                                      const std::vector<unsigned int>& mask) {
    return { 
            &cols, 
            &mask,
            nullptr,
            nullptr
           };
}

inline GetDataframeFilterIdxSimd get_dataframe_filter_idx_simd(
                   std::initializer_list<size_t> cols,
                   std::initializer_list<unsigned int> mask) {

    auto owned_cols = std::make_shared<std::vector<size_t>>      (cols);
    auto owned_mask = std::make_shared<std::vector<unsigned int>>(mask);

    return { 
            owned_cols.get(), 
            owned_mask.get(),
            owned_cols,
            owned_mask
           };
}

inline GetDataframeFilterIdxSimd get_dataframe_filter_idx_simd(
                   std::initializer_list<size_t> cols,
                   const std::vector<unsigned int> mask) {

    auto owned_cols = std::make_shared<std::vector<size_t>>(cols);

    return { 
            owned_cols.get(), 
            &mask,
            owned_cols,
            nullptr
           };
}

inline GetDataframeFilterIdxSimd get_dataframe_filter_idx_simd(
                   const std::vector<size_t> cols,
                   std::initializer_list<unsigned int> mask) {

    auto owned_mask = std::make_shared<std::vector<unsigned int>>(mask);

    return { 
            &cols, 
            owned_mask.get(),
            nullptr,
            owned_mask
           };
}

template<typename DF>
auto operator|(const DF& df, const GetDataframeFilterIdxSimd& g) {
    return g(df);  
}


