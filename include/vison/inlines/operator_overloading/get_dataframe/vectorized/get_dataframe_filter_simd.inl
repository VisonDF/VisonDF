#pragma once

struct GetDataframeFilterSimd {
    const std::vector<size_t>*  cols;
    const std::vector<uint8_t>* mask;

    std::shared_ptr<std::vector<size_t>>  owned_cols;
    std::shared_ptr<std::vector<uint8_t>> owned_mask;

    template<typename DF>
    auto operator()(const DF& df) const {
        DF out;
        df.get_dataframe_filter_simd(*cols, out, *mask);
        return out;
    }
};

inline GetDataframeFilterSimd get_dataframe_filter_simd(const std::vector<size_t>& cols,
                                                        const std::vector<uint8_t>& mask) {
    return GetDataframeFilterSimd{ 
             &cols, 
             &mask,
             nullptr,
             nullptr
    };
}

inline GetDataframeFilterSimd get_dataframe_filter_simd(std::initializer_list<size_t> cols,
                                                        std::initializer_list<uint8_t> mask)
{
    auto owned_cols = std::make_shared<std::vector<size_t>> (cols);
    auto owned_mask = std::make_shared<std::vector<uint8_t>>(mask);

    return GetDataframeFilterSimd{
        owned_cols.get(),   
        owned_mask.get(),
        owned_cols,
        owned_mask
    };
}

inline GetDataframeFilterSimd get_dataframe_filter_simd(std::initializer_list<size_t> cols,
                                                        const std::vector<uint8_t>& mask)
{
    auto owned_cols  = std::make_shared<std::vector<size_t>>(cols);

    return GetDataframeFilterSimd{
        owned_cols.get(),   
        &mask,
        owned_cols,
        nullptr
    };
}

inline GetDataframeFilterSimd get_dataframe_filter_simd(const std::vector<size_t> cols,
                                                        std::initializer_list<uint8_t>& mask)
{
    auto owned_mask  = std::make_shared<std::vector<uint8_t>>(mask);

    return GetDataframeFilterSimd{
        &cols,   
        owned_mask.get(),
        nullptr,
        owned_mask
    };
}

template<typename DF>
auto operator|(const DF& df, const GetDataframeFilterSimd& g) {
    return g(df);  
}


