#pragma once

template <bool IB>
struct GetColFilterIdx {
    unsigned int idx;
    const std::vector<unsigned int>* mask;

    template<typename T = void, typename DF>
    auto operator()(const DF& df) const {
        std::vector<T> out;
        df.template get_col_filter_idx<T, IB>(idx, out, *mask);
        return out;
    }
};

template <bool IB = false>
inline GetColFilterIdx<IB> get_col_filter_idx(unsigned int idx,
                                          const std::vector<unsigned int>& mask) 
{ 
        return GetColFilterIdx<IB>{idx, &mask}; 
}

template <typename T = void, 
          typename DF, 
          bool IB>
auto operator|(const DF& df, const GetColFilterIdx<IB>& g) {
    
    return g.template operator()<T>(df);
}




