#pragma once

template <typename TColVal>
void dispatch_alrd(auto&& f,
                   const size_t start,
                   const size_t end,
                   const size_t val_idx,
                   const std::vector<unsigned int>& grp_by,
                   auto& var_vec_grp
                  )
{

    if constexpr (!std:is_same_v<TColVal, void>) {

        if constexpr (std::is_same_v<TColVal, std::string>) {

            const auto& val_table = str_v[val_idx];

            f(start,
              end,
              val_idx,
              grp_by,
              vec,
              val_table
              );

        } else if constexpr (std::is_same_v<TColVal, CharT>) {

            const auto& val_table = chr_v[val_idx];

            f(start,
              end,
              val_idx,
              grp_by,
              vec,
              val_table
              );

        } else if constexpr (std::is_same_v<TColVal, uint8_t>) {

            const auto& val_table = bool_v[val_idx];

            f(start,
              end,
              val_idx,
              grp_by,
              vec,
              val_table
              );

        } else if constexpr (std::is_same_v<TColVal, IntT>) {

            const auto& val_table = int_v[val_idx];

            f(start,
              end,
              val_idx,
              grp_by,
              vec,
              val_table
              );

        } else if constexpr (std::is_same_v<TColVal, UIntT>) {

            const auto& val_table = uint_v[val_idx];

            f(start,
              end,
              val_idx,
              grp_by,
              vec,
              val_table
              );

        } else {

            const auto& val_table = str_v[val_idx];

            f(start,
              end,
              val_idx,
              grp_by,
              vec,
              val_table
              );

        }

    } else {

        std::visit([](auto&& vec) {

            using TP = std::decay_t<decltype(vec)>;
            if constexpr (!std::is_same_v<TP, std::monostate>) {

                using Elem = element_type_t<TP::value_type>;

                if constexpr (std::is_same_v<Elem, std::string>) {

                    const auto& val_table = str_v[val_idx];

                    f(start,
                      end,
                      val_idx,
                      grp_by,
                      vec,
                      val_table
                      );

                } else if constexpr (std::is_same_v<Elem, CharT>) {

                    const auto& val_table = chr_v[val_idx];

                    f(start,
                      end,
                      val_idx,
                      grp_by,
                      vec,
                      val_table
                      );


                } else if constexpr (std::is_same_v<Elem, uint8_t>) {

                    const auto& val_table = bool_v[val_idx];

                    f(start,
                      end,
                      val_idx,
                      grp_by,
                      vec,
                      val_table
                      );

                } else if constexpr (std::is_same_v<Elem, IntT>) {

                    const auto& val_table = int_v[val_idx];

                    f(start,
                      end,
                      val_idx,
                      grp_by,
                      vec,
                      val_table
                      );

                } else if constexpr (std::is_same_v<Elem, UIntT>) {

                    const auto& val_table = uint_v[val_idx];

                    f(start,
                      end,
                      val_idx,
                      grp_by,
                      vec,
                      val_table
                      );

                } else {

                    const auto& val_table = dbl_v[val_idx];

                    f(start,
                      end,
                      val_idx,
                      grp_by,
                      vec,
                      val_table
                      );

                }

            }


        }, var_vec_grp);

    }

}






