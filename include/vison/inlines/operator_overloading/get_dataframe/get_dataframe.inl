#pragma once

struct GetDataframe {
    const std::vector<size_t>* cols;
    std::shared_ptr<std::vector<size_t>>  owned_cols;

    template<typename DF>
    auto operator()(const DF& df) const {
        DF out;
        df.get_dataframe(*cols, out);
        return out;
    }
};

inline GetDataframe get_dataframe(const std::vector<size_t>& cols) {
    return { &cols, nullptr };
}

inline GetDataframe get_dataframe(std::initializer_list<size_t> il)
{
    auto owned_cols  = std::make_shared<std::vector<size_t>>(il);
    return GetDataframe{ owned_cols.get(), 
                         owned_cols 
                        };   
}

template<typename DF>
auto operator|(const DF& df, const GetDataframe& g) {
    return g(df);  
}


