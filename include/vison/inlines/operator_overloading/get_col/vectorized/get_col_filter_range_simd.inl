#pragma once

template <bool IB>
struct GetColFilterRangeSimd {
    unsigned int idx;
    const std::vector<uint8_t>* mask;
    unsigned int strt_vl;

    std::shared_ptr<std::vector<uint8_t>> owned_mask;

    template<typename T = void, typename DF>
    auto operator()(const DF& df) const {
        std::vector<T> out;
        df.template get_col_filter_range_simd<T, IB>(idx, out, *mask, strt_vl);
        return out;
    }
};

template <bool IB = false>
inline GetColFilterRangeSimd<IB> get_col_filter_range(unsigned int idx,
                                                  const std::vector<uint8_t>& mask,
                                                  const unsigned int strt_vl) 
{ 
        return GetColFilterRangeSimd<IB>{
                                          idx, 
                                          &mask, 
                                          strt_vl,
                                          nullptr
                                        }; 
}

template <bool IB = false>
inline GetColFilterRangeSimd<IB> get_col_filter_range(unsigned int idx,
                                                  std::initializer_list<uint8_t> mask,
                                                  const unsigned int strt_vl) 
{ 
        auto owned_mask = std::make_shared<std::vector<uint8_t>>(mask);
        return GetColFilterRangeSimd<IB>{idx, 
                                         owned_mask.get(), 
                                         strt_vl,
                                         owned_mask
                                        }; 
}

template <typename T = void, 
          typename DF, 
          bool IB>
auto operator|(const DF& df, const GetColFilterRangeSimd<IB>& g) 
{    
    return g.template operator()<T>(df);
}




