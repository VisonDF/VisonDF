#pragma once

struct OccLookupOneColHard {
    template <typename TContainer,
              [[unused]] typename FunctionKey>
    static void apply(const size_t start, 
                      const size_t end, 
                      auto& cmap,
                      const auto& val_struct,
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

            for (unsigned int i = start; i < end; ++i) {
                auto [it, inserted] = cmap.try_emplace(key_col[i], val_struct);
                auto& cur_struct = it->second;
                ++cur_struct.value;
                cur_struct.idx_vec.v.push_back(i);
            }

        } else {

            for (unsigned int i = start; i < end; ++i) {
                auto [it, inserted] = cmap.try_emplace(std::string_view{
                                                          reinterpret_cast<const char*>(&key_col[i]),
                                                          val_size}, val_struct);
                auto& cur_struct = it->second;
                ++cur_struct.value;
                cur_struct.idx_vec.v.push_back(i);
            }

        }
    };
}

struct AddLookupOneColHard {
    template <typename Tcontainer,
              [[unused]] typename FunctionKey>
    static void apply(const auto& val_col, 
                      const size_t start, 
                      const size_t end, 
                      auto& cmap,
                      const auto& val_struct,
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

            for (unsigned int i = start; i < end; ++i) {
                auto [it, inserted] = cmap.try_emplace(key_col[i], val_struct);
                auto& cur_struct = it->second;
                cur_struct.value += val_col[i];
                cur_struct.idx_vec.v.push_back(i);
            }

        } else {

            for (unsigned int i = start; i < end; ++i) {
                auto [it, inserted] = cmap.try_emplace(std::string_view{
                                                          reinterpret_cast<const char*>(&key_col[i]),
                                                          val_size}, val_struct);
                auto& cur_struct = it->second;
                cur_struct.value += val_col[i];
                cur_struct.idx_vec.v.push_back(i);
            }

        }
    };
}

struct FillLookupOneColHard {
    template <typename TContainer,
              [[unused]] typename FunctionKey>
    static void apply(const auto& val_col, 
                      size_t start, 
                      size_t end, 
                      auto& cmap,
                      const auto& vec_struct,
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

             for (unsigned int i = start; i < end; ++i) {
                  auto [it, inserted] = cmap.try_emplace(key_col[i], vec_struct);
                  auto& cur_struct = it->second;
                  cur_struct.value.v.push_back(val_col[i]);
                  cur_struct.idx_vec.v.push_back(i);
             }

        } else {

            for (unsigned int i = start; i < end; ++i) {
                 auto [it, inserted] = cmap.try_emplace(std::string_view{
                                                         reinterpret_cast<const char*>(&key_col[i]),
                                                         val_size}, vec_struct);
                 auto& cur_struct = it->second;
                 cur_struct.value.v.push_back(val_col[i]);
                 cur_struct.idx_vec.v.push_back(i);
            }

        }
    };
}



