#pragma once

template <typename TContainer>
inline unsigned int key_table_build(auto& key_table, const unsigned int x)
{

    unsigned int idx_type;

    if constexpr (!std::is_same_v<TContainer, void>) {

        if constexpr (std::is_same_v<element_type_t<TContainer>, std::string>) {

            key_table = &str_v;
            idx_type = 0;
        
        } else if constexpr (std::is_same_v<element_type_t<TContainer>, CharT>) {

            key_table = &chr_v;
            idx_type = 1;
        
        } else if constexpr (std::is_same_v<element_type_t<TContainer>, uint8_t>) {

            key_table = &bool_v;
            idx_type = 2;
            
        } else if constexpr (std::is_same_v<element_type_t<TContainer>, IntT>) {

            key_table = &int_v;
            idx_type = 3;
            
        } else if constexpr (std::is_same_v<element_type_t<TContainer>, UIntT>) {

            key_table = &uint_v;
            idx_type = 4;
            
        } else if constexpr (std::is_same_v<element_type_t<TContainer>, FloatT>) {

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

    return iex_type;

}



