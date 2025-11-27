#pragma once

struct GetDataframeFilter {
    const std::vector<size_t>*  cols;
    const std::vector<uint8_t>* mask;

    template<typename DF>
    auto operator()(const DF& df) const {
        DF out;
        df.get_dataframe_filter(*cols, out, *mask);
        return out;
    }
};

inline GetDataframeFilter get_dataframe_filter(const std::vector<size_t>& cols,
                                               const std::vector<uint8_t>& mask) {
    return { &cols, &mask };
}

template<typename DF>
auto operator|(const DF& df, const GetDataframeFilter& g) {
    return g(df);  
}


