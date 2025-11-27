#pragma once

template <bool IB>
struct GetColFilterIdxSimd {
    unsigned int idx;
    const std::vector<unsigned int>* mask;

    std::shared_ptr<std::vector<unsigned int>> owned_mask;

    template<typename T = void, typename DF>
    auto operator()(const DF& df) const {
        std::vector<T> out;
        df.template get_col_filter_idx_simd<T, IB>(idx, out, *mask);
        return out;
    }
};

template <bool IB = false>
inline GetColFilterIdxSimd<IB> get_col_filter_idx_simd(unsigned int idx,
                                                       const std::vector<unsigned int>& mask) 
{ 
        return GetColFilterIdxSimd<IB>{
                                        idx, 
                                        &mask,
                                        nullptr
                                      }; 
}

template <bool IB = false>
inline GetColFilterIdxSimd<IB> get_col_filter_idx_simd(unsigned int idx,
                                                       std::initializer_list<unsigned int>& mask) 
{
        auto owned_mask = std::make_shared<std::vector<unsigned int>>(mask);
        return GetColFilterIdxSimd<IB>{
                                        idx, 
                                        owned_mask.get(),
                                        owned_mask
                                      }; 
}


template <typename T = void, 
          typename DF, 
          bool IB>
auto operator|(const DF& df, const GetColFilterIdxSimd<IB>& g) {
    
    return g.template operator()<T>(df);
}




