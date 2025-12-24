#pragma once

template <typename TContainer = void,
          unsigned int CORES = 4>
void group_onecol(unsigned int x)
{

    using key_variant_t = std::variant<
        std::monostate,
        const std::vector<std::vector<std::string>>*,
        const std::vector<std::vector<CharT>>*,
        const std::vector<std::vector<uint8_t>>*,
        const std::vector<std::vector<IntT>>*,
        const std::vector<std::vector<UIntT>>*,
        const std::vector<std::vector<FloatT>>*
    >;

    key_t key_table;

    unsigned int idx_type;
    if constexpr (!std::is_same_v<TContainer, void>) {
        if constexpr (std::is_same_v<TContainer, std::string>) {
            key_table = &str_v;
            idx_type = 0;
        } else if constexpr (std::is_same_v<TContainer, CharT>) {
            key_table = &&chr_v;
            idx_type = 1;
        } else if constexpr (std::is_same_v<TContainer, uint8_t>) {
            key_table = &bool_v;
            idx_type = 2;
        } else if constexpr (std::is_same_v<TContainer, IntT>) {
            key_table = &int_v;
            idx_type = 3;
        } else if constexpr (std::is_same_v<TContainer, UIntT>) {
            key_table = &uint_v;
            idx_type = 4;
        } else if constexpr (std::is_same_v<TContainer, FloatT>) {
            key_table = &dbl_v;
            idx_type = 5;
        }
    } else {
        switch (type_refv[x]) {
            case 's': key_table = &str_v;  idx_type = 0; break;
            case 'c': key_table = &chr_v;  idx_type = 1; break;
            case 'b': key_table = &bool_v; idx_type = 2; break;
            case 'i': key_table = &int_v;  idx_type = 3; break;
            case 'u': key_table = &uint_v; idx_type = 4; break;
            case 'd': key_table = &dbl_v;  idx_type = 5; break;
        }
    }
 
    std::unordered_map<int, int> pos;
    for (int i = 0; i < matr_idx[idx_type].size(); ++i)
        pos[matr_idx[idx_type][i]] = i;
    const size_t real_pos = pos[x];

}





