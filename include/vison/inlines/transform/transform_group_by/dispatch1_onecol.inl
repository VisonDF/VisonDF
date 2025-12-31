#pragma once

template <GroupFunction Function,
          unsigned int Nb,
          typename TContainer,
          typename TColVal,
          typename FunctionLoop,
          typename FunctionKey>
inline void dispatch1_onecol(const size_t start, 
                             const size_t end, 
                             auto& cmap,
                             const size_t val_size,
                             const size_t key_idx,
                             const size_t val_idx,
                             const auto& var_key_table,
                             const auto& var_val_table,
                             const size_t NPerGroup,
                             auto& key_vec) {

    if constexpr (!std::is_same_v<Tcontainer, void>) {

        using Elem = element_type_t<TContainer>;

        if constexpr (std::is_same_v<Elem, std::string>) {

                const auto& key_col = str_v[key_idx];
                dispatch2<Function, 
                          Nb,
                          TColVal,
                          Elem, // TContainer
                          FunctionLoop,
                          FunctionKey>(start, 
                                       end, 
                                       cmap, 
                                       key_col, 
                                       val_size,
                                       key_idx,
                                       val_idx,
                                       var_val_table,
                                       NPerGroup,
                                       idx,
                                       idx,
                                       idx,
                                       idx,
                                       idx,
                                       idx,
                                       key_vec
                                       );

            } else if constexpr (std::is_same_v<Elem, CharT>) {

                const auto& key_col = chr_v[key_idx];
                dispatch2<Function, 
                          Nb, 
                          TColVal,
                          Elem, // TContainer
                          FunctionLoop,
                          FunctionKey>(start, 
                                       end, 
                                       cmap, 
                                       key_col, 
                                       val_size,
                                       key_idx,
                                       val_idx,
                                       var_val_table,
                                       NPerGroup,
                                       idx,
                                       idx,
                                       idx,
                                       idx,
                                       idx,
                                       idx,
                                       key_vec
                                       );

            } else if constexpr (std::is_same_v<Elem, uint8_t>) {

                const auto& key_col = bool_v[key_idx];
                dispatch2<Function, 
                          Nb,
                          TColVal,
                          Elem, // TContainer
                          FunctionLoop,
                          FunctionKey>(start, 
                                       end, 
                                       cmap, 
                                       key_col, 
                                       val_size,
                                       key_idx,
                                       val_idx,
                                       var_val_table,
                                       NPerGroup,
                                       idx,
                                       idx,
                                       idx,
                                       idx,
                                       idx,
                                       idx,
                                       key_vec
                                       );

            } else if constexpr (std::is_same_v<Elem, IntT>) {

                const auto& key_col = int_v[key_idx];
                dispatch2<Function, 
                          Nb,
                          TColVal,
                          Elem, // TContainer
                          FunctionLoop,
                          FunctionKey>(start, 
                                       end, 
                                       cmap, 
                                       key_col, 
                                       val_size,
                                       key_idx,
                                       val_idx,
                                       var_val_table,
                                       NPerGroup,
                                       idx,
                                       idx,
                                       idx,
                                       idx,
                                       idx,
                                       idx,
                                       key_vec
                                       );

            } else if constexpr (std::is_same_v<Elem, UIntT>) {

                const auto& key_col = uint_v[key_idx];
                dispatch2<Function, 
                          Nb,
                          TColVal,
                          Elem, // TContainer
                          FunctionLoop,
                          FunctionKey>(start, 
                                       end, 
                                       cmap, 
                                       key_col, 
                                       val_size,
                                       key_idx,
                                       val_idx,
                                       var_val_table,
                                       NPerGroup,
                                       idx,
                                       idx,
                                       idx,
                                       idx,
                                       idx,
                                       idx,
                                       key_vec
                                       );

            } else if constexpr (std::is_same_v<Elem, FloatT>) {

                const auto& key_col = dbl_v[key_idx];
                dispatch2<Function, 
                          Nb,
                          TColVal,
                          Elem, // TContainer
                          FunctionLoop,
                          FunctionKey>(start, 
                                       end, 
                                       cmap, 
                                       key_col, 
                                       val_size,
                                       key_idx,
                                       val_idx
                                       var_val_table,
                                       NPerGroup,
                                       idx,
                                       idx,
                                       idx,
                                       idx,
                                       idx,
                                       idx,
                                       key_vec
                                       );

            }

    } else {

        std::visit([&](auto&& tbl_ptr) {

            using TP = std::decay_t<decltype(tbl_ptr)>;

            if constexpr (!std::is_same_v<TP, std::monostate>) {

                using Elem = std::remove_cvref_t<decltype(tbl_ptr)>::value_type::value_type;

                if constexpr (std::is_same_v<Elem, std::string>) {

                    const auto& key_col = str_v[key_idx];
                    dispatch2<Function, 
                              Nb, 
                              TColVal,
                              Elem, // TContainer
                              FunctionLoop,
                              FunctionKey>(start, 
                                           end, 
                                           cmap, 
                                           key_col, 
                                           val_size,
                                           key_idx,
                                           val_idx,
                                           var_val_table,
                                           NPerGroup,
                                           idx,
                                           idx,
                                           idx,
                                           idx,
                                           idx,
                                           idx,
                                           key_vec
                                           );

                } else if constexpr (std::is_same_v<Elem, CharT>) {

                    const auto& key_col = chr_v[key_idx];
                    dispatch2<Function, 
                              Nb,
                              TColVal,
                              Elem, // TContainer
                              FunctionLoop,
                              FunctionKey>(start, 
                                           end, 
                                           cmap, 
                                           key_col, 
                                           val_size,
                                           key_idx,
                                           val_idx,
                                           var_val_table,
                                           NPerGroup,
                                           idx,
                                           idx,
                                           idx,
                                           idx,
                                           idx,
                                           idx,
                                           key_vec
                                           );

                } else if constexpr (std::is_same_v<Elem, uint8_t>) {

                    const auto& key_col = bool_v[key_idx];
                    dispatch2<Function, 
                              Nb, 
                              TColVal,
                              Elem, // TContainer
                              FunctionLoop,
                              FunctionKey>(start, 
                                           end, 
                                           cmap, 
                                           key_col, 
                                           val_size,
                                           key_idx,
                                           val_idx,
                                           var_val_table,
                                           NPerGroup,
                                           idx,
                                           idx,
                                           idx,
                                           idx,
                                           idx,
                                           idx,
                                           key_vec
                                           );

                } else if constexpr (std::is_same_v<Elem, IntT>) {

                    const auto& key_col = int_v[key_idx];
                    dispatch2<Function, 
                              Nb,
                              TColVal,
                              Elem, // TContainer
                              FunctionLoop,
                              FunctionKey>(start, 
                                           end, 
                                           cmap, 
                                           key_col, 
                                           val_size,
                                           key_idx,
                                           val_idx,
                                           var_val_table,
                                           NPerGroup,
                                           idx,
                                           idx,
                                           idx,
                                           idx,
                                           idx,
                                           idx,
                                           key_vec
                                           );

                } else if constexpr (std::is_same_v<Elem, UIntT>) {

                    const auto& key_col = uint_v[key_idx];
                    dispatch2<Function, 
                              Nb,
                              TColVal,
                              Elem, // TContainer
                              FunctionLoop,
                              FunctionKey>(start, 
                                           end, 
                                           cmap, 
                                           key_col, 
                                           val_size,
                                           key_idx,
                                           val_idx,
                                           var_val_table,
                                           NPerGroup,
                                           idx,
                                           idx,
                                           idx,
                                           idx,
                                           idx,
                                           idx,
                                           key_vec
                                           );

                } else if constexpr (std::is_same_v<Elem, FloatT>) {

                    const auto& key_col = dbl_v[key_idx];
                    dispatch2<Function, 
                              Nb,
                              TColVal,
                              Elem, // TContainer
                              FunctionLoop,
                              FunctionKey>(start, 
                                           end, 
                                           cmap, 
                                           key_col, 
                                           val_size,
                                           key_idx,
                                           val_idx,
                                           var_val_table,
                                           NPerGroup,
                                           idx,
                                           idx,
                                           idx,
                                           idx,
                                           idx,
                                           idx,
                                           key_vec
                                           );

                }
            }
         }, var_key_table);
    }
};




