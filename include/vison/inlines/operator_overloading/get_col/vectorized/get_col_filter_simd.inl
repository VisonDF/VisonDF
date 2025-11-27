#pragma once

template <bool IB>
struct GetColFilterSimd {
    unsigned int idx;
    const std::vector<uint8_t>* mask;

    template<typename T = void, typename DF>
    auto operator()(const DF& df) const {
        std::vector<T> out;
        df.template get_col_filter_simd<T, IB>(idx, out, *mask);
        return out;
    }
};

template <bool IB = false>
inline GetColFilterSimd<IB> get_col_filter_simd(unsigned int idx,
                                                const std::vector<uint8_t>& mask) 
{ 
        return GetColFilterSimd<IB>{idx, &mask}; 
}

template <typename T = void, 
          typename DF, 
          bool IB>
auto operator|(const DF& df, const GetColFilterSimd<IB>& g) 
{    
    return g.template operator()<T>(df);
}


