#pragma once

struct GetDataframeFilterSimd {
    const std::vector<size_t>*  cols;
    const std::vector<uint8_t>* mask;

    template<typename DF>
    auto operator()(const DF& df) const {
        DF out;
        df.get_dataframe_filter_simd(*cols, out, *mask);
        return out;
    }
};

inline GetDataframeFilterSimd get_dataframe_filter_simd(const std::vector<size_t>& cols,
                                                   const std::vector<uint8_t>& mask) {
    return { &cols, &mask };
}

template<typename DF>
auto operator|(const DF& df, const GetDataframeFilterSimd& g) {
    return g(df);  
}


