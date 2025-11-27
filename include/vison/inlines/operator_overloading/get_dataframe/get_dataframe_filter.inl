#pragma once

struct GetDataframeFilter {
    const std::vector<size_t>*  cols;
    const std::vector<uint8_t>* mask;

    std::shared_ptr<std::vector<size_t>>  owned_cols;
    std::shared_ptr<std::vector<uint8_t>> owned_mask;

    template<typename DF>
    auto operator()(const DF& df) const {
        DF out;
        df.get_dataframe_filter(*cols, out, *mask);
        return out;
    }
};

inline GetDataframeFilter get_dataframe_filter(const std::vector<size_t>& cols,
                                               const std::vector<uint8_t>& mask) {
    return { &cols, 
             &mask,
             nullptr,
             nullptr
    };
}

inline GetDataframeFilter get_dataframe_filter(std::initializer_list<size_t> cols,
                                               std::initializer_list<uint8_t> mask)
{
    auto owned_cols  = std::make_shared<std::vector<size_t>>(cols);
    auto owned_mask = std::make_shared<std::vector<uint8_t>>(mask);

    return GetDataframeFilter{
        owned_cols.get(),   
        owned_mask.get(),
        owned_cols,
        owned_mask
    };
}

inline GetDataframeFilter get_dataframe_filter(std::initializer_list<size_t> cols,
                                               const std::vector<uint8_t>& mask)
{
    auto owned_cols  = std::make_shared<std::vector<size_t>>(cols);

    return GetDataframeFilter{
        owned_cols.get(),   
        &mask,
        owned_cols,
        nullptr
    };
}

inline GetDataframeFilter get_dataframe_filter(const std::vector<size_t> cols,
                                               std::initializer_list<uint8_t>& mask)
{
    auto owned_mask  = std::make_shared<std::vector<uint8_t>>(mask);

    return GetDataframeFilter{
        &cols,   
        owned_mask.get(),
        nullptr,
        owned_mask
    };
}

template<typename DF>
auto operator|(const DF& df, const GetDataframeFilter& g) {
    return g(df);  
}



