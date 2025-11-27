#pragma once

struct GetDataframeFilterIdx {
    const std::vector<size_t>*       cols;
    const std::vector<unsigned int>* mask;

    template<typename DF>
    auto operator()(const DF& df) const {
        DF out;
        df.get_dataframe_filter_idx(*cols, out, *mask);
        return out;
    }
};

inline GetDataframeFilterIdx get_dataframe_filter_idx(const std::vector<size_t>& cols,
                                                      const std::vector<unsigned int>& mask) {
    return { &cols, &mask };
}

template<typename DF>
auto operator|(const DF& df, const GetDataframeFilterIdx& g) {
    return g(df);  
}


