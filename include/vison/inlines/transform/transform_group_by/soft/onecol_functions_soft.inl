#pragma once

struct OneColSoft {
    template <typename TContainer, 
              [[unused]] typename FunctionKey>
    static void apply ([[unused]] const auto& val_col, 
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
                       [[unused]] const auto& key_vec
                       ) 
    {
        if constexpr (std::is_same_v<TContainer, std::string>) {

            for (unsigned int i = 0; i < local_nrow; ++i) {    
                auto [it, inserted] = lookup.try_emplace(key_col[i], midx_vec);
                it->second.v.push_back(i);
            }

        } else {

            for (unsigned int i = 0; i < local_nrow; ++i) {    
                auto [it, inserted] = lookup.try_emplace(std::string_view{reinterpret_cast<const char*>(&key_col[i]), 
                                                         val_size}, 
                                                         midx_vec);
                it->second.v.push_back(row_view_idx[i]);
            }

        }
    }
}



