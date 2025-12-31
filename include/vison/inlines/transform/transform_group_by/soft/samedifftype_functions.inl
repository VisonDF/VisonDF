#pragma once

struct SameDiffTypeSoft {
    template <typename TContainer,
              typename FunctionKey> 
    static void apply ([[unused]] const auto& val_col, 
                       size_t start, 
                       size_t end, 
                       auto& cmap,
                       const auto& vec,
                       [[unused]] const auto& key_col,
                       const size_t val_size,
                       std::vector<unsigned int>& key_idx,
                       const std::vector<unsigned int>& idx_str,
                       const std::vector<unsigned int>& idx_chr,
                       const std::vector<unsigned int>& idx_bool,
                       const std::vector<unsigned int>& idx_int,
                       const std::vector<unsigned int>& idx_uint,
                       const std::vector<unsigned int>& idx_dbl,
                       [[unused]] const auto& key_vec
                       ) 
    {

        std::string key;
        key.reserve(2048);      

        for (unsigned int i = start; i < end; ++i) {
            key.clear();
 
            FunctionKey::template apply<TContainer>(key, 
                                                    i,
                                                    key_idx,
                                                    val_size,
                                                    idx_str,
                                                    idx_chr,
                                                    idx_bool,
                                                    idx_int,
                                                    idx_uint,
                                                    idx_dbl);

            auto [it, inserted] = cmap.try_emplace(key, vec);
            it->second.v.push_back(i);
        }
    }
}



