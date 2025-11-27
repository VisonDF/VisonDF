#pragma once

struct GetDataframeFilterRange {
    const std::vector<size_t>*  cols;
    const std::vector<uint8_t>* mask;
    const size_t strt_vl;

    template<typename DF>
    auto operator()(const DF& df) const {
        DF out;
        df.get_dataframe_filter_range(*cols, out, *mask, strt_vl);
        return out;
    }
};

inline GetDataframeFilterRange get_dataframe_filter_range(const std::vector<size_t>& cols,
                                                          const std::vector<uint8_t>& mask,
                                                          const size_t strt_vl) {
    return { &cols, &mask, strt_vl};
}

template<typename DF>
auto operator|(const DF& df, const GetDataframeFilterRange& g) {
    return g(df);  
}


