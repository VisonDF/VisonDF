#pragma once

template <bool IB>
struct GetCol {
    unsigned int idx;

    template<typename T = void, typename DF>
    auto operator()(const DF& df) const {
        std::vector<T> out;
        df.template get_col<T, IB>(idx, out);
        return out;
    }
};

template <bool IB = false>
inline GetCol<IB> get_col(unsigned int idx) 
{ 
        return GetCol<IB>{idx}; 
}

template <typename T = void, 
          typename DF, 
          bool IB>
auto operator|(const DF& df, const GetCol<IB>& g) 
{    
    return g.template operator()<T>(df);
}


