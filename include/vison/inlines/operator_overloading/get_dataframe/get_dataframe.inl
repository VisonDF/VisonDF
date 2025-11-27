#pragma once

struct GetDataframe {
    const std::vector<size_t>* cols;

    template<typename DF>
    auto operator()(const DF& df) const {
        DF out;
        df.get_dataframe(*cols, out);
        return out;
    }
};

inline GetDataframe get_dataframe(const std::vector<size_t>& cols) {
    return { &cols };
}

template<typename DF>
auto operator|(const DF& df, const GetDataframe& g) {
    return g(df);  
}


