#pragma once

struct GetCol {
    unsigned int idx;

    template<typename T = void, typename DF>
    auto operator()(const DF& df) const {
        return df.template get_col<T>(idx);
    }
};

inline GetCol get_col(unsigned int idx) { return GetCol{idx}; }

template <typename T = void, typename DF>
auto operator|(const DF& df, const GetCol& g) {
    
    std::vector<T> out;
    df.template get_col<T>(g.idx, out);

    return out;
}














