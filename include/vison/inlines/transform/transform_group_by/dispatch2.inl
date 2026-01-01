#pragma once

template <GroupFunction Function,
          unsigned int Nb,
          typename TColVal,
          typename TContainer,
          typename FunctionLoop,
          typename FunctionKey>
inline void dispatch2(const size_t start,
                      const size_t end,
                      auto& cmap,
                      const auto& key_col,
                      const size_t val_size,
                      const auto& key_idx,
                      const size_t val_idx,
                      const auto& var_val_table,
                      const size_t NPerGroup,
                      const std::vector<unsigned int>& idx_str,
                      const std::vector<unsigned int>& idx_chr,
                      const std::vector<unsigned int>& idx_bool,
                      const std::vector<unsigned int>& idx_int,
                      const std::vector<unsigned int>& idx_uint,
                      const std::vector<unsigned int>& idx_dbl,
                      auto& key_vec
                      ) {

    if constexpr (!std::is_same_v<TColVal, void>) {

        if constexpr (std::is_same_v<TColVal, std::string>) {

            const auto& val_col = str_v[val_idx];
            dispatch3<Function, 
                      Nb,
                      TContainer,
                      FunctionLoop,
                      FunctionKey>(start, 
                                   end, 
                                   cmap, 
                                   key_col, 
                                   val_size,
                                   val_col,
                                   NPerGroup,
                                   key_idx,
                                   idx_str,
                                   idx_chr,
                                   idx_bool,
                                   idx_int,
                                   idx_uint,
                                   idx_dbl,
                                   key_vec
                                   );

        } else if constexpr (std::is_same_v<TColVal, CharT>) {

            const auto& val_col = chr_v[val_idx];
            dispatch3<Function, 
                      Nb,
                      TContainer,
                      FunctionLoop,
                      FunctionKey>(start, 
                                   end, 
                                   cmap, 
                                   key_col, 
                                   val_size,
                                   val_col,
                                   NPerGroup,
                                   key_idx,
                                   idx_str,
                                   idx_chr,
                                   idx_bool,
                                   idx_int,
                                   idx_uint,
                                   idx_dbl,
                                   key_vec
                                   );

        } else if constexpr (std::is_same_v<TColVal, uint8_t>) {

            const auto& val_col = bool_v[val_idx];
            dispatch3<Function, 
                      Nb,
                      TContainer,
                      FunctionLoop,
                      FunctionKey>(start, 
                                   end, 
                                   cmap, 
                                   key_col, 
                                   val_size,
                                   val_col,
                                   NPerGroup,
                                   key_idx,
                                   idx_str,
                                   idx_chr,
                                   idx_bool,
                                   idx_int,
                                   idx_uint,
                                   idx_dbl,
                                   key_vec
                                   );

        } else if constexpr (std::is_same_v<TColVal, IntT>) {

            const auto& val_col = int_v[val_idx];
            dispatch3<Function, 
                      Nb,
                      TContainer,
                      FunctionLoop,
                      FunctionKey>(start, 
                                   end, 
                                   cmap, 
                                   key_col, 
                                   val_size,
                                   val_col,
                                   NPerGroup,
                                   key_idx,
                                   idx_str,
                                   idx_chr,
                                   idx_bool,
                                   idx_int,
                                   idx_uint,
                                   idx_dbl,
                                   key_vec
                                   );

        } else if constexpr (std::is_samee_v<TColVal, UIntT>) {

            const auto& val_col = uint_v[val_idx];
            dispatch3<Function, 
                      Nb,
                      TContainer,
                      FunctionLoop,
                      FunctionKey>(start, 
                                   end, 
                                   cmap, 
                                   key_col, 
                                   val_size,
                                   val_col,
                                   NPerGroup,
                                   key_idx,
                                   idx_str,
                                   idx_chr,
                                   idx_bool,
                                   idx_int,
                                   idx_uint,
                                   idx_dbl,
                                   key_vec
                                   );

        } else {

            const auto& val_col = dbl_v[val_idx];
            dispatch3<Function, 
                      Nb,
                      TContainer,
                      FunctionLoop,
                      FunctionKey>(start, 
                                   end, 
                                   cmap, 
                                   key_col, 
                                   val_size,
                                   val_col,
                                   NPerGroup,
                                   key_idx,
                                   idx_str,
                                   idx_chr,
                                   idx_bool,
                                   idx_int,
                                   idx_uint,
                                   idx_dbl,
                                   key_vec
                                   );

        }

    } else if constexpr (Function == GroupFunction::Occurence) {

        std::vector<uint8_t> val_col = {};
        dispatch3<Function, 
                  Nb,
                  TContainer,
                  FunctionLoop,
                  FunctionKey>(start, 
                               end, 
                               cmap, 
                               key_col, 
                               val_size,
                               val_col,
                               NPerGroup,
                               key_idx,
                               idx_str,
                               idx_chr,
                               idx_bool,
                               idx_int,
                               idx_uint,
                               idx_dbl,
                               key_vec
                               );

    } else {

        std::visit([&](auto&& tbl_ptr) {

            using TP  = std::decay_t<decltype(tbl_ptr)>;

            if constexpr (!std::is_same_v<TP, std::monostate>) {
                
                const auto& val_col = (*tbl_ptr)[val_idx];
                using Elem = TP::value_type::value_type;

                if constexpr (Function != GroupFunction::Gather) {

                    if constexpr (std::is_same_v<Elem, std::string>) {

                        auto& cur_map = std::get<1>(cmap);
                        dispatch3<Function, 
                                  Nb,
                                  TContainer,
                                  FunctionLoop,
                                  FunctionKey>(start, 
                                               end, 
                                               cur_map, 
                                               key_col, 
                                               val_size,
                                               val_col,
                                               NPerGroup,
                                               key_idx,
                                               idx_str,
                                               idx_chr,
                                               idx_bool,
                                               idx_int,
                                               idx_uint,
                                               idx_dbl,
                                               key_vec
                                               );

                    } else if constexpr (std::is_same_v<Elem, CharT>) {

                        auto& cur_map = std::get<2>(cmap);
                        dispatch3<Function, 
                                  Nb,
                                  TContainer,
                                  FunctionLoop,
                                  FunctionKey>(start, 
                                               end, 
                                               cur_map, 
                                               key_col, 
                                               val_size,
                                               val_col,
                                               NPerGroup,
                                               key_idx,
                                               idx_str,
                                               idx_chr,
                                               idx_bool,
                                               idx_int,
                                               idx_uint,
                                               idx_dbl,
                                               key_vec
                                               );

                    } else if constexpr (std::is_same_v<Elem, uint8_t>) {

                        auto& cur_map = std::get<3>(cmap);
                        dispatch3<Function, 
                                  Nb,
                                  TContainer,
                                  FunctionLoop,
                                  FunctionKey>(start, 
                                               end, 
                                               cur_map, 
                                               key_col, 
                                               val_size,
                                               val_col,
                                               NPerGroup,
                                               key_idx,
                                               idx_str,
                                               idx_chr,
                                               idx_bool,
                                               idx_int,
                                               idx_uint,
                                               idx_dbl,
                                               key_vec
                                               );

                    } else if constexpr (std::is_same_v<Elem, IntT>) {

                        auto& cur_map = std::get<4>(cmap);
                        dispatch3<Function, 
                                  Nb,
                                  TContainer,
                                  FunctionLoop,
                                  FunctionKey>(start, 
                                               end, 
                                               cur_map, 
                                               key_col, 
                                               val_size,
                                               val_col,
                                               NPerGroup,
                                               key_idx,
                                               idx_str,
                                               idx_chr,
                                               idx_bool,
                                               idx_int,
                                               idx_uint,
                                               idx_dbl,
                                               key_vec
                                               );

                    } else if constexpr (std::is_same_v<Elem, UIntT>) {

                        auto& cur_map = std::get<5>(cmap);
                        dispatch3<Function, 
                                  Nb,
                                  TContainer,
                                  FunctionLoop,
                                  FunctionKey>(start, 
                                               end, 
                                               cur_map, 
                                               key_col, 
                                               val_size,
                                               val_col,
                                               NPerGroup,
                                               key_idx,
                                               idx_str,
                                               idx_chr,
                                               idx_bool,
                                               idx_int,
                                               idx_uint,
                                               idx_dbl,
                                               key_vec
                                               );

                    } else {

                        auto& cur_map = std::get<6>(cmap);
                        dispatch3<Function, 
                                  Nb,
                                  TContainer,
                                  FunctionLoop,
                                  FunctionKey>(start, 
                                               end, 
                                               cur_map, 
                                               key_col, 
                                               val_size,
                                               val_col,
                                               NPerGroup,
                                               key_idx,
                                               idx_str,
                                               idx_chr,
                                               idx_bool,
                                               idx_int,
                                               idx_uint,
                                               idx_dbl,
                                               key_vec
                                               );

                    }

                } else {

                    if constexpr (std::is_same_v<Elem, std::string>) {

                        auto& cur_map = std::get<7>(cmap);
                        dispatch3<Function, 
                                  Nb,
                                  TContainer,
                                  FunctionLoop,
                                  FunctionKey>(start, 
                                               end, 
                                               cur_map, 
                                               key_col, 
                                               val_size,
                                               val_col,
                                               NPerGroup,
                                               key_idx,
                                               idx_str,
                                               idx_chr,
                                               idx_bool,
                                               idx_int,
                                               idx_uint,
                                               idx_dbl,
                                               key_vec
                                               );

                    } else if constexpr (std::is_same_v<Elem, CharT>) {

                        auto& cur_map = std::get<8>(cmap);
                        dispatch3<Function, 
                                  Nb,
                                  TContainer,
                                  FunctionLoop,
                                  FunctionKey>(start, 
                                               end, 
                                               cur_map, 
                                               key_col, 
                                               val_size,
                                               val_col,
                                               NPerGroup,
                                               key_idx,
                                               idx_str,
                                               idx_chr,
                                               idx_bool,
                                               idx_int,
                                               idx_uint,
                                               idx_dbl,
                                               key_vec
                                               );

                    } else if constexpr (std::is_same_v<Elem, uint8_t>) {

                        auto& cur_map = std::get<9>(cmap);
                        dispatch3<Function, 
                                  Nb,
                                  TContainer,
                                  FunctionLoop,
                                  FunctionKey>(start, 
                                               end, 
                                               cur_map, 
                                               key_col, 
                                               val_size,
                                               val_col,
                                               NPerGroup,
                                               key_idx,
                                               idx_str,
                                               idx_chr,
                                               idx_bool,
                                               idx_int,
                                               idx_uint,
                                               idx_dbl,
                                               key_vec
                                               );

                    } else if constexpr (std::is_same_v<Elem, IntT>) {

                        auto& cur_map = std::get<10>(cmap);
                        dispatch3<Function, 
                                  Nb,
                                  TContainer,
                                  FunctionLoop,
                                  FunctionKey>(start, 
                                               end, 
                                               cur_map, 
                                               key_col, 
                                               val_size,
                                               val_col,
                                               NPerGroup,
                                               key_idx,
                                               idx_str,
                                               idx_chr,
                                               idx_bool,
                                               idx_int,
                                               idx_uint,
                                               idx_dbl,
                                               key_vec
                                               );

                    } else if constexpr (std::is_same_v<Elem, UIntT>) {

                        auto& cur_map = std::get<11>(cmap);
                        dispatch3<Function, 
                                  Nb,
                                  TContainer,
                                  FunctionLoop,
                                  FunctionKey>(start, 
                                               end, 
                                               cur_map, 
                                               key_col, 
                                               val_size,
                                               val_col,
                                               NPerGroup,
                                               key_idx,
                                               idx_str,
                                               idx_chr,
                                               idx_bool,
                                               idx_int,
                                               idx_uint,
                                               idx_dbl,
                                               key_vec
                                               );

                    } else {

                        auto& cur_map = std::get<12>(cmap);
                        dispatch3<Function, 
                                  Nb,
                                  TContainer,
                                  FunctionLoop,
                                  FunctionKey>(start, 
                                               end, 
                                               cur_map, 
                                               key_col, 
                                               val_size,
                                               val_col,
                                               NPerGroup,
                                               key_idx,
                                               idx_str,
                                               idx_chr,
                                               idx_bool,
                                               idx_int,
                                               idx_uint,
                                               idx_dbl,
                                               key_vec
                                               );

                    }

                }

            }

        }, var_val_table);

    }
}




