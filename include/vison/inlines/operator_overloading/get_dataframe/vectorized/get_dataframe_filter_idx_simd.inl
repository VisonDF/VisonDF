#pragma once

struct GetDataframeFilterIdxSimd {
    const std::vector<size_t>*       cols;
    const std::vector<unsigned int>* mask;

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
    return { &cols, &mask };
}

template<typename DF>
auto operator|(const DF& df, const GetDataframeFilterIdxSimd& g) {
    return g(df);  
}


