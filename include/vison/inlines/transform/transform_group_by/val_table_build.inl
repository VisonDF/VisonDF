#pragma once

template <typename TColVal, 
          GroupFunction Function,
          bool MapCol,
          typename F
          >
inline void val_table_build(unsigned int& idx_type, 
                            unsigned int& val_idx,
                            unsigned int& pre_idx_type,
                            const size_t n)
{

    if constexpr (!Occurence) {

        if constexpr (!std::is_same_v<TColVal, void>) {

            if constexpr (std::is_same_v<element_type_t<TColVal>, std::string>) {

                val_table = &str_v;
                idx_type = 0;

            } else if constexpr (std::is_same_v<element_type_t<TColVal>, CharT>) {

                val_table = &chr_v;
                idx_type = 1;

            } else if constexpr (std::is_same_v<element_type_t<TColVal>, uint8_t>) {

                val_table = &bool_v;
                idx_type = 2;

            } else if constexpr (std::is_same_v<element_type_t<TColVal>, IntT>) {

                val_table = &int_v;
                idx_type = 3;

            } else if constexpr (std::is_same_v<element_type_t<TColVal>, UIntT>) {

                val_table = &uint_v;
                idx_type = 4;

            } else if constexpr (std::is_same_v<element_type_t<TColVal>, FloatT>) {

                val_table = &dbl_v;
                idx_type = 5;

            }

        } else {

            switch (n) {
                case 0: val_table = &str_v;  idx_type = 0; break;
                case 1: val_table = &chr_v;  idx_type = 1; break;
                case 2: val_table = &bool_v; idx_type = 2; break;
                case 3: val_table = &int_v;  idx_type = 3; break;
                case 4: val_table = &uint_v; idx_type = 4; break;
                case 5: val_table = &dbl_v;  idx_type = 5; break;
            }

        }

        pre_idx_type = idx_type;

        if constexpr (!MapCol) {

            auto it = std::find(matr_idx[idx_type].begin(), matr_idx[idx_type].end(), n_col);
            if (it != matr_idx[idx_type].end()) {
                val_idx = std::distance(matr_idx[idx_type].begin(), it);
                break;
            } else {
                std::cerr << "`TColVal` type missmatch\n";
                return;
            }

        } else {

            if (matr_idx_map[idx_type].empty()) {
                std::cerr << "MapCol mode but no col found in matr_idx_map[idx_type]\n";
                return;
            }
            val_idx = matr_idx_map[idx_type][n];

        }

        if constexpr (Function == GroupFunction::Gather) {

            if constexpr (!std::is_same_v<TColVal, void>) {
                using R = std::remove_cvref_t<
                    std::invoke_result_t<F, std::vector<TColVal>&>
                >;
                if constexpr (std::is_same_v<R, std::string>) {
                    idx_type = 0;
                } else if constexpr (std::is_same_v<R, CharT>) {
                    idx_type = 1;
                } else if constexpr (std::is_same_v<R, uint8_t>) {
                    idx_type = 2;
                } else if constexpr (std::is_same_v<R, IntT>) {
                    idx_type = 3;
                } else if constexpr (std::is_same_v<R, UIntT>) {
                    idx_type = 4;
                } else (std::is_same_v<R, FloatT>) {
                    idx_type = 5;
                }

            } else {

                std::visit([&idx_type](auto&& ptr) {

                   using TP = std::decay_t<decltype(ptr)>;

                   if constexpr (!std::is_same_v<TP, std::monostate>) {
                       using Elem = std::invoke_result_t<F, std::vector<TP::value_type::value_type>&>;
                       if constexpr (std::is_same_v<Elem, std::string>) {
                           idx_type = 0;
                       } else if constexpr (std::is_same_v<Elem, CharT>) {
                           idx_type = 1;
                       } else if constexpr (std::is_same_v<Elem, uint8_t>) {
                           idx_type = 2;
                       } else if constexpr (std::is_same_v<Elem, IntT>) {
                           idx_type = 3;
                       } else if constexpr (std::is_same_v<Elem, UIntT>) {
                           idx_type = 4;
                       } else {
                           idx_type = 5;
                       }
                   }

                }, val_table);

            }
        }

    } else {

        idx_type = 4;
        pre_idx_type = idx_type;

    }
}





