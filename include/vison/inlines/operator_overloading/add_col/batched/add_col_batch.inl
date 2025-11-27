#pragma once

template <typename T, bool BoolAsU8, unsigned int BATCH>
struct AddColBatch {
    const std::vector<T>* x;      
    const std::string* name;      

    std::shared_ptr<std::vector<T>>  owned_x;     
    std::shared_ptr<std::string>     owned_name;  

    template<typename DF>
    DF& operator()(DF& df) const {
        df.template add_col_batch<T, BoolAsU8, BATCH>(*x, *name);
        return df;   
    }
};

template <typename T, 
          bool BoolAsU8 = false, 
          unsigned int BATCH = 256>
inline AddColBatch<T, BoolAsU8, BATCH> add_col_batch(const std::vector<T>& x,
                                                     const std::string& name)
{
    return AddColBatch<T, BoolAsU8, BATCH>{
        &x,
        &name,
        nullptr,
        nullptr
    };
}

template <typename T, 
          bool BoolAsU8 = false, 
          unsigned int BATCH = 256>
inline AddColBatch<T, BoolAsU8, BATCH> add_col_batch(std::initializer_list<T> x,
                                                     const std::string& name)
{
    auto owned_x = std::make_shared<std::vector<T>>(x);

    return AddColBatch<T, BoolAsU8, BATCH>{
        owned_x.get(),
        &name,
        owned_x,
        nullptr
    };
}

template <typename T, 
          bool BoolAsU8 = false, 
          unsigned int BATCH = 256>
inline AddColBatch<T, BoolAsU8, BATCH> add_col_batch(const std::vector<T>& x,
                                                     const char* name_cstr)
{
    auto owned_name = std::make_shared<std::string>(name_cstr);

    return AddColBatch<T, BoolAsU8, BATCH>{
        &x,
        owned_name.get(),
        nullptr,
        owned_name
    };
}

template <typename T, 
          bool BoolAsU8 = false, 
          unsigned int BATCH = 256>
inline AddColBatch<T, BoolAsU8, BATCH> add_col_batch(std::initializer_list<T> x,
                                                     const char* name_cstr)
{
    auto owned_x    = std::make_shared<std::vector<T>>(x);
    auto owned_name = std::make_shared<std::string>(name_cstr);

    return AddColBatch<T, BoolAsU8, BATCH>{
        owned_x.get(),
        owned_name.get(),
        owned_x,
        owned_name
    };
}

template <typename DF, 
          typename T, 
          bool BoolAsU8,
          unsigned int BATCH>
inline DF& operator|(DF& df, const AddColBatch<T, BoolAsU8, BATCH>& g) {
    return g(df);  
}



