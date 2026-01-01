#pragma once

template <typename TColVal,
          unsigned int CORES,
          GroupFunction Function,
          typename FunctionMerge
         >
inline void dispatch_merge_alrd(
                                const size_t unique_grps,
                                const size_t chunks,
                                const std::vector<unsigned int>& grp_by,
                                const auto& var_val_grp_vec,
                                auto& var_val_grp
                                )
{
    if constexpr (!std::is_same_v<TColVal, void>) {

        FunctionMerge::template apply<CORES, Function>(unique_grps,
                                                       chunks,
                                                       grp_by,
                                                       var_val_grp_vec,
                                                       var_val_grp,
                                                       );

    } else {

        std::visit([](auto&& val_grp) {

            using TP = std::decay_t<decltype(val_grp)>;
            if constexpr (!std::is_same_v<TP, std::monostate>) {

                auto& val_grp_vec = std::get<std::vector<TP>>(var_val_grp_vec);

                FunctionMerge::template apply<CORES, Function>(unique_grps,
                                                               chunks,
                                                               grp_by,
                                                               val_grp_vec,
                                                               val_grp,
                                                               );

            }

        }, var_val_grp);

    }
}



