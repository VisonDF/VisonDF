#pragma once

template <typename T, bool Large, bool BoolAsU8>
struct AddCol {
    const std::vector<T>* x;      
    const std::string* name;      

    std::shared_ptr<std::vector<T>>  owned_x;     
    std::shared_ptr<std::string>     owned_name;  

    template<typename DF>
    DF& operator()(DF& df) const {
        df.template add_col<T, Large, BoolAsU8>(*x, *name);
        return df;   
    }
};

template <typename T, bool Large = false, bool BoolAsU8 = false>
inline AddCol<T, Large, BoolAsU8> add_col(const std::vector<T>& x,
                                          const std::string& name)
{
    return AddCol<T, Large, BoolAsU8>{
        &x,
        &name,
        nullptr,
        nullptr
    };
}

template <typename T, bool Large = false, bool BoolAsU8 = false>
inline AddCol<T, Large, BoolAsU8> add_col(std::initializer_list<T> x,
                                          const std::string& name)
{
    auto owned_x = std::make_shared<std::vector<T>>(x);

    return AddCol<T, Large, BoolAsU8>{
        owned_x.get(),
        &name,
        owned_x,
        nullptr
    };
}

template <typename T, bool Large = false, bool BoolAsU8 = false>
inline AddCol<T, Large, BoolAsU8> add_col(const std::vector<T>& x,
                                          const char* name_cstr)
{
    auto owned_name = std::make_shared<std::string>(name_cstr);

    return AddCol<T, Large, BoolAsU8>{
        &x,
        owned_name.get(),
        nullptr,
        owned_name
    };
}

template <typename T, bool Large = false, bool BoolAsU8 = false>
inline AddCol<T, Large, BoolAsU8> add_col(std::initializer_list<T> x,
                                          const char* name_cstr)
{
    auto owned_x    = std::make_shared<std::vector<T>>(x);
    auto owned_name = std::make_shared<std::string>(name_cstr);

    return AddCol<T, Large, BoolAsU8>{
        owned_x.get(),
        owned_name.get(),
        owned_x,
        owned_name
    };
}

template <typename DF, typename T, bool Large, bool BoolAsU8>
inline DF& operator|(DF& df, const AddCol<T, Large, BoolAsU8>& g) {
    return g(df);  
}



