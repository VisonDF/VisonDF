#pragma once

template <bool MC, bool IB>
struct GetColFilter {
    unsigned int idx;
    const std::vector<uint8_t>* mask;

    std::shared_ptr<std::vector<uint8_t>> owned_mask;

    template<typename T = void, typename DF>
    auto operator()(const DF& df) const {
        std::vector<T> out;
        df.template get_col_filter<T, MC, IB>(idx, out, *mask);
        return out;
    }
};

template <bool MC = false, bool IB = false>
inline GetColFilter<MC, IB> get_col_filter(unsigned int idx,
                                           const std::vector<uint8_t>& mask) 
{ 
        return GetColFilter<MC, IB>{idx, 
                                    &mask,
                                    nullptr
                                   }; 
}

template <bool MC = false, bool IB = false>
inline GetColFilter<MC, IB> get_col_filter(unsigned int idx,
                                           std::initializer_list<uint8_t> mask) 
{ 
        auto owned_mask = std::make_shared<std::vector<uint8_t>>(mask);
        return GetColFilter<MC, IB>{idx, 
                                    owned_mask.get(),
                                    owned_mask
                                   }; 
}

template <typename T = void, 
          typename DF, 
          bool MC, 
          bool IB>
auto operator|(const DF& df, const GetColFilter<MC, IB>& g) 
{    
    return g.template operator()<T>(df);
}


