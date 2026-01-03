#pragma once


struct OccLookupOneCol {
    template <typename TContainer
              [[unused]] typename FunctionKey>
    static apply(const size_t start, 
                 const size_t end, 
                 auto& cmap,
                 const auto& zero,
                 const auto& key_col,
                 const size_t val_size,
                 [[unused]] const auto& key_idx,
                 [[unused]] const std::vector<unsigned int>& idx_str,
                 [[unused]] const std::vector<unsigned int>& idx_chr,
                 [[unused]] const std::vector<unsigned int>& idx_bool,
                 [[unused]] const std::vector<unsigned int>& idx_int,
                 [[unused]] const std::vector<unsigned int>& idx_uint,
                 [[unused]] const std::vector<unsigned int>& idx_dbl,
                 auto& key_vec
                 ) 
    {

    if constexpr (std::is_same_v<TConatiner, std::string>) {

        for (unsigned int i = start; i < end; ++i) {
            auto [it, inserted] = cmap.try_emplace(key_col[i], zero);
            ++it->second;
            key_vec[i] = &it->first;
        }

    } else {

            for (unsigned int i = start; i < end; ++i) {
                auto [it, inserted] = cmap.try_emplace(std::string_view{
                                                        reinterpret_cast<const char*>(&key_col[i]),
                                                         val_size}, zero);
                ++it->second;
                key_vec[i] = &it->first;
            }

        }
    }
};


struct AddLookupOneCol {
    template <typename TContainer,
             [[unused]] typename FunctionKey>
        static void apply(const auto& val_col, 
                          const size_t start, 
                          const size_t end, 
                          auto& cmap,
                          const auto& zero,
                          const auto& key_col,
                          const size_t val_size,
                          [[unused]] const auto& key_idx,
                          [[unused]] const std::vector<unsigned int>& idx_str,
                          [[unused]] const std::vector<unsigned int>& idx_chr,
                          [[unused]] const std::vector<unsigned int>& idx_bool,
                          [[unused]] const std::vector<unsigned int>& idx_int,
                          [[unused]] const std::vector<unsigned int>& idx_uint,
                          [[unused]] const std::vector<unsigned int>& idx_dbl,
                          auto& key_vec
                          ) 
        {
        if constexpr (std::is_same_v<TConatiner, std::string>) {

            for (unsigned int i = start; i < end; ++i) {
                auto [it, inserted] = cmap.try_emplace(key_col[i], zero);
                (it->second) += val_col[i];
                key_vec[i] = &it->first;
            }

        } else {

            for (unsigned int i = start; i < end; ++i) {
                auto [it, inserted] = cmap.try_emplace(std::string_view{
                                                        reinterpret_cast<const char*>(&key_col[i]),
                                                         val_size}, zero);
                (it->second) += val_col[i];
                key_vec[i] = &it->first;
            }

        }
    };
}

    
struct FillLookupOneCol {
    template <typename TContainer,
              [[unused]] typename FunctionKey>
    static void apply (const auto& val_col, 
                       size_t start, 
                       size_t end, 
                       auto& cmap,
                       const auto& vec,
                       const auto& key_col,
                       const size_t val_size,
                       [[unused]] const auto& key_idx,
                       [[unused]] const std::vector<unsigned int>& idx_str,
                       [[unused]] const std::vector<unsigned int>& idx_chr,
                       [[unused]] const std::vector<unsigned int>& idx_bool,
                       [[unused]] const std::vector<unsigned int>& idx_int,
                       [[unused]] const std::vector<unsigned int>& idx_uint,
                       [[unused]] const std::vector<unsigned int>& idx_dbl,
                       auto& key_vec
                       ) 
    {
        if constexpr (std::is_same_v<TContainer, std::string>) {

            for (unsigned int i = start; i < end; ++i) {
                auto [it, inserted] = cmap.try_emplace(key_col[i], vec);
                it->second.v.push_back(val_col[i]);
                key_vec[i] = &it->first;
            }

        } else {

            for (unsigned int i = start; i < end; ++i) {
                auto [it, inserted] = cmap.try_emplace(std::string_view{
                                                        reinterpret_cast<const char*>(&key_col[i]),
                                                         val_size}, vec);
                it->second.v.push_back(val_col[i]);
                key_vec[i] = &it->first;
            }

        }
    };
}



