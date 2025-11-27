#pragma once

struct GetDataframeFilterRangeSimd {
    const std::vector<size_t>*  cols;
    const std::vector<uint8_t>* mask;
    const size_t strt_vl;

    template<typename DF>
    auto operator()(const DF& df) const {
        DF out;
        df.get_dataframe_filter_range_simd(*cols, out, *mask, strt_vl);
        return out;
    }
};

inline GetDataframeFilterRangeSimd get_dataframe_filter_range_simd(
                                                          const std::vector<size_t>& cols,
                                                          const std::vector<uint8_t>& mask,
                                                          const size_t strt_vl) {
    return { &cols, &mask, strt_vl};
}

template<typename DF>
auto operator|(const DF& df, const GetDataframeFilterRangeSimd& g) {
    return g(df);  
}


