#pragma once

template <GroupFunction Function,
          typename CreatePolicy,
          typename TColVal>
inline void dispatch_merge_hard(auto& vec_map,
                                auto& var_lookup,
                                const size_t NPerGroup,
                                auto& key_table2) {

    if constexpr (!std::is_same_v<TColVal, void>) {

        using Elem = element_type_t<TColVal>;
        PairGroupBy<Elem>               zero_struct(NPerGroup);
        PairGroupBy<ReservingVec<Elem>> vec_struct(NPerGroup);

        CreatePolicy::template apply<Function>(zero_struct,
                                               vec_struct,
                                               cur_map,
                                               lookup);

    } else {

        std::visit([&](auto&& tbl_ptr) {

           using TP = std::decay_t<decltype(tbl_ptr)>;

           if constexpr (!std::is_same_v<TP, std::monostate>) {

               using Elem = TP::value_type::value_type;
               PairGroupBy<Elem>               zero_struct(NPerGroup);
               PairGroupBy<ReservingVec<Elem>> vec_struct(NPerGroup);

               for (const auto& cmap : vec_map) {
                
                   if constexpr (Function != GroupFunction::Gather) {

                       if constexpr (std::is_same_v<Elem, std::string>) {

                           const auto& cur_map = std::get<1>(cmap);
                           auto& lookup        = std::get<1>(var_lookup);
                           CreatePolicy::template apply<Function>(zero_struct, 
                                                                  vec_struct, 
                                                                  cur_map, 
                                                                  lookup);

                       } else if constexpr (std::is_same_v<Elem, CharT>) {

                           const auto& cur_map = std::get<2>(cmap);
                           auto& lookup        = std::get<2>(var_lookup);
                           CreatePolicy::template apply<Function>(zero_struct, 
                                                                  vec_struct, 
                                                                  cur_map, 
                                                                  lookup);

                       } else if constexpr (std::is_same_v<Elem, uint8_t>) {

                           const auto& cur_map = std::get<3>(cmap);
                           auto& lookup        = std::get<3>(var_lookup);
                           CreatePolicy::template apply<Function>(zero_struct, 
                                                                  vec_struct, 
                                                                  cur_map, 
                                                                  lookup);

                       } else if constexpr (std::is_same_v<Elem, IntT>) {

                           const auto& cur_map = std::get<4>(cmap);
                           auto& lookup        = std::get<4>(var_lookup);
                           CreatePolicy::template apply<Function>(zero_struct, 
                                                                  vec_struct, 
                                                                  cur_map, 
                                                                  lookup);

                       } else if constexpr (std::is_same_v<Elem, UIntT>) {

                           const auto& cur_map = std::get<5>(cmap);
                           auto& lookup        = std::get<5>(var_lookup);
                           CreatePolicy::template apply<Function>(zero_struct, 
                                                                  vec_struct, 
                                                                  cur_map, 
                                                                  lookup);

                       } else {

                           const auto& cur_map = std::get<6>(cmap);
                           auto& lookup        = std::get<6>(var_lookup);
                           CreatePolicy::template apply<Function>(zero_struct, 
                                                                  vec_struct, 
                                                                  cur_map, 
                                                                  lookup);

                       }

                   } else {

                       if constexpr (std::is_same_v<Elem, std::string>) {

                           const auto& cur_map = std::get<7>(cmap);
                           auto& lookup        = std::get<7>(var_lookup);
                           CreatePolicy::template apply<Function>(zero_struct, 
                                                                  vec_struct, 
                                                                  cur_map, 
                                                                  lookup);

                       } else if constexpr (std::is_same_v<Elem, CharT>) {

                           const auto& cur_map = std::get<8>(cmap);
                           auto& lookup        = std::get<8>(var_lookup);
                           CreatePolicy::template apply<Function>(zero_struct, 
                                                                  vec_struct, 
                                                                  cur_map, 
                                                                  lookup);

                       } else if constexpr (std::is_same_v<Elem, uint8_t>) {

                           const auto& cur_map = std::get<9>(cmap);
                           auto& lookup        = std::get<9>(var_lookup);
                           CreatePolicy::template apply<Function>(zero_struct, 
                                                                  vec_struct, 
                                                                  cur_map, 
                                                                  lookup);

                       } else if constexpr (std::is_same_v<Elem, IntT>) {

                           const auto& cur_map = std::get<10>(cmap);
                           auto& lookup        = std::get<10>(var_lookup);
                           CreatePolicy::template apply<Function>(zero_struct, 
                                                                  vec_struct, 
                                                                  cur_map, 
                                                                  lookup);

                       } else if constexpr (std::is_same_v<Elem, UIntT>) {

                           const auto& cur_map = std::get<11>(cmap);
                           auto& lookup        = std::get<11>(var_lookup);
                           CreatePolicy::template apply<Function>(zero_struct, 
                                                                  vec_struct, 
                                                                  cur_map, 
                                                                  lookup);

                       } else {

                           const auto& cur_map = std::get<12>(cmap);
                           auto& lookup        = std::get<12>(var_lookup);
                           CreatePolicy::template apply<Function>(zero_struct, 
                                                                  vec_struct, 
                                                                  cur_map, 
                                                                  lookup);

                       }

                   }
               }
           }
        } , key_table2);

    }
};



