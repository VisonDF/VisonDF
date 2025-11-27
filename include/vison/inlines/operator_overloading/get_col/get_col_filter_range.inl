#pragma once

template <bool MC, bool IB>
struct GetColFilterRange {
    unsigned int idx;
    const std::vector<uint8_t>* mask;
    unsigned int strt_vl;

    template<typename T = void, typename DF>
    auto operator()(const DF& df) const {
        std::vector<T> out;
        df.template get_col_filter_range<T, MC, IB>(idx, out, *mask, strt_vl);
        return out;
    }
};

template <bool MC = false, bool IB = false>
inline GetColFilterRange<MC, IB> get_col_filter_range(unsigned int idx,
                                                      const std::vector<uint8_t>& mask,
                                                      const unsigned int strt_vl) 
{ 
        return GetColFilterRange<MC, IB>{idx, &mask, strt_vl}; 
}

template <typename T = void, 
          typename DF, 
          bool MC, 
          bool IB>
auto operator|(const DF& df, const GetColFilterRange<MC, IB>& g) 
{    
    return g.template operator()<T>(df);
}




