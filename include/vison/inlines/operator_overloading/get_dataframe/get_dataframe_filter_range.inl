#pragma once

struct GetDataframeFilterRange {
    const std::vector<size_t>*  cols;
    const std::vector<uint8_t>* mask;
    const size_t strt_vl;

    std::shared_ptr<std::vector<size_t>>  owned_cols;
    std::shared_ptr<std::vector<uint8_t>> owned_mask;

    template<typename DF>
    auto operator()(const DF& df) const {
        DF out;
        df.get_dataframe_filter_range(*cols, out, *mask, strt_vl);
        return out;
    }
};

inline GetDataframeFilterRange get_dataframe_filter_range(const std::vector<size_t>& cols,
                                                          const std::vector<uint8_t>& mask,
                                                          const size_t strt_vl) {
    return { &cols, 
             &mask, 
             strt_vl,
             nullptr,
             nullptr
    };
}

inline GetDataframeFilterRange get_dataframe_filter_range(
                std::initializer_list<size_t>& cols,
                std::initializer_list<uint8_t>& mask,
                const size_t strt_vl) {
   
    auto owned_cols = std::make_shared<std::vector<size_t>> (cols);
    auto owned_mask = std::make_shared<std::vector<uint8_t>>(mask);

    return { owned_cols.get(), 
             owned_mask.get(), 
             strt_vl,
             owned_cols,
             owned_mask
    };
}

inline GetDataframeFilterRange get_dataframe_filter_range(
                std::initializer_list<size_t>& cols,
                const std::vector<uint8_t>& mask,
                const size_t strt_vl) {

    auto owned_cols = std::make_shared<std::vector<size_t>>(cols);

    return { owned_cols.get(), 
             &mask, 
             strt_vl,
             owned_cols,
             nullptr
    };
}

inline GetDataframeFilterRange get_dataframe_filter_range(
                const std::vector<size_t>& cols,
                std::initializer_list<uint8_t>& mask,
                const size_t strt_vl) {

    auto owned_mask = std::make_shared<std::vector<uint8_t>>(mask);

    return { &cols, 
             owned_mask.get(), 
             strt_vl,
             nullptr,
             owned_mask
    };
}

template<typename DF>
auto operator|(const DF& df, const GetDataframeFilterRange& g) {
    return g(df);  
}


