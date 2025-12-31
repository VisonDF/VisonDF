#pragma once

struct KeyBuildSameType {
    template <typename TContainer>
    inline void apply(std::string& key, 
                      const unsigned int i,
                      const std::vector<unsigned int>& key_idx,
                      const size_t val_size,
                      [[unused]] const std::vector<unsigned int>& idx_str,
                      [[unused]] const std::vector<unsigned int>& idx_chr,
                      [[unused]] const std::vector<unsigned int>& idx_bool,
                      [[unused]] const std::vector<unsigned int>& idx_int,
                      [[unused]] const std::vector<unsigned int>& idx_uint,
                      [[unused]] const std::vector<unsigned int> idx_dbl
                      ) 
    {
        if constexpr (!std::is_same_v<TContainer, std::string>) {
            if constexpr (std::is_same_v<TContainer, CharT>) {
                for (size_t j = 0; j < idx.size(); ++j) {
                    key.append(
                        (*key_table)[key_idx[j]][i],
                        val_size
                    );
                    key.push_back('\x1F');
                }
            } else {
                for (size_t j = 0; j < idx.size(); ++j) {
                    const auto& v = (*key_table)[key_idx[j]][i]; 
                    key.append(
                        reinterpret_cast<const char*>(std::addressof(v)),
                        val_size
                    );
                    key.push_back('\x1F');
                }
            }
        } else {
            for (size_t j = 0; j < idx.size(); ++j) {
                const std::string& src = (*key_table)[key_idx[j]][i];
                key.append(src.data(), src.size()); 
                key.push_back('\x1F');
            }
        }
    }
}


struct KeyBuildDiffType {
    template <[[unused]] typename TContainer>
    inline void apply(std::string& key, 
                      const unsigned int i,
                      [[unused]] const std::vector<unsigned int>& key_idx,
                      [[unused]] const size_t val_size,
                      const std::vector<unsigned int>& idx_str,
                      const std::vector<unsigned int>& idx_chr,
                      const std::vector<unsigned int>& idx_bool,
                      const std::vector<unsigned int>& idx_int,
                      const std::vector<unsigned int>& idx_uint,
                      const std::vector<unsigned int> idx_dbl
                     ) 
    {
        for (auto idxv : idx_str) {
            const auto& v = str_v[idxv][i];
            key.append(
                       v.data(),
                       v.size()
            );
            key.push_back('\x1F');              
        }
        for (auto idxv : idx_chr) {
            const auto& v = chr_v[idxv][i];
            key.append(
                       v,
                       sizeof(CharT)
            );
            key.push_back('\x1F');              
        }
        for (auto idxv : idx_bool) {
            const auto& v = bool_v[idxv][i];
            key.append(
                       reinterpret_cast<const char*>(&v),
                       sizeof(uint8_t)
            );
            key.push_back('\x1F');              
        }
        for (auto idxv : idx_int) {
            const auto& v = int_v[idxv][i];
            key.append(
                       reinterpret_cast<const char*>(&v),
                       sizeof(IntT)
            );
            key.push_back('\x1F');              
        }
        for (auto idxv : idx_uint) {
            const auto& v = uint_v[idxv][i];
            key.append(
                       reinterpret_cast<const char*>(&v),
                       sizeof(UIntT)
            );
            key.push_back('\x1F');              
        }
        for (auto idxv : idx_dbl) {
            const auto& v = dbl_v[idxv][i];
            key.append(
                       reinterpret_cast<const char*>(&v),
                       sizeof(FloatT)
            );
            key.push_back('\x1F');              
        }
    }
}



