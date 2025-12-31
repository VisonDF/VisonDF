#pragma once

struct OccLookupSameDiffTypeHard {
    template <typename TContainer,
              typename FunctionKey>
    static void apply(size_t start, 
                      size_t end, 
                      auto& cmap,
                      const auto& val_struct,
                      [[unused]] const auto& key_col,
                      const size_t val_size,
                      const std::vector<unsigned int>& key_idx,
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

            auto [it, inserted] = cmap.try_emplace(key, val_struct);
            auto& cur_struct = it->second;
            ++cur_struct.value;
            cur_struct.idx_vec.v.push_back(i);
        }
    };
}


struct AddLookupSameDiffTypeHard {
    template <typename TContainer, 
              typename FunctionKey>
    static void apply(const auto& val_col, 
                      const size_t start, 
                      const size_t end, 
                      auto& cmap,
                      const auto& val_struct,
                      [[unused]] const auto& key_col,
                      const size_t val_size,
                      const std::vector<unsigned int>& key_idx,
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

            auto [it, inserted] = cmap.try_emplace(key, val_struct);
            auto& cur_struct = it->second;
            cur_struct.value += val_col[i];
            cur_struct.idx_vec.v.push_back(i);
        }
    };
}

struct FilLookupSameDiffTypeHard {
    template <typename TContainer,
              typename FunctionKey>
    static void apply(const auto& val_col, 
                      size_t start, 
                      size_t end, 
                      auto& cmap,
                      const auto& vec_struct,
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

             auto [it, inserted] = cmap.try_emplace(key, vec_struct);
             auto& cur_struct = it->second;
             cur_struct.value.v.push_back(val_col[i]);
             cur_struct.idx_vec.v.push_back(i);
        }
    };
}




