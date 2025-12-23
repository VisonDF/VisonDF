#pragma once

template <typename TContainer = void,
          typename TColVal = void,
          unsigned int CORES = 4,
          GroupFunction Function = GroupFunction::Occurence,
          bool SimdHash  = true,
          unsigned int NPerGroup = 4,
          typename F = decltype(&default_groupfn_impl)>
requires Group<F, first_arg_grp_t<F>>
void transform_group_by_hard_mt(const std::vector<unsigned int>& x,
                                const n_col int,
                                const std::string colname = "n",
                                const F f = &default_groupfn_impl) 
{

    if (in_view) {
        std::cerr << "Can't use this operation while in `view` mode, " 
                  << "consider applying `.materialize()`\n";
        return;
    }

    if constexpr (std::is_same_v<TColVal, void>) {

        switch(type_refv[n]) {
            case 's': group_by_dispatch1_hard<TContainer, 
                                              std::string, 
                                              CORES, 
                                              Function, 
                                              SimdHash, 
                                              NPerGroup, 
                                              F>(x, ncol, colname, f); break;
            case 'c': group_by_dispatch1_hard<TContainer, 
                                              CharT, 
                                              CORES, 
                                              Function, 
                                              SimdHash, 
                                              NPerGroup, 
                                              F>(x, ncol, colname, f); break;
            case 'b': group_by_dispatch1_hard<TContainer, 
                                              uint8_t, 
                                              CORES, 
                                              Function, 
                                              SimdHash, 
                                              NPerGroup, 
                                              F>(x, ncol, colname, f); break;
            case 'i': group_by_dispatch1_hard<TContainer, 
                                              IntT, 
                                              CORES, 
                                              Function, 
                                              SimdHash, 
                                              NPerGroup, 
                                              F>(x, ncol, colname, f); break;
            case 'u': group_by_dispatch1_hard<TContainer, 
                                              UIntT, 
                                              CORES, 
                                              Function, 
                                              SimdHash, 
                                              NPerGroup, 
                                              F>(x, ncol, colname, f); break;
            case 'd': group_by_dispatch1_hard<TContainer, 
                                              FloatT, 
                                              CORES, 
                                              Function, 
                                              SimdHash, 
                                              NPerGroup, 
                                              F>(x, ncol, colname, f); break;
        }

    } else {
        if (x.size() > 1) {
            const char ref_t = type_refv[x[0]];
            for (auto& ii : x) {
                if (type_refv[ii] != ref_t) {
                    transform_group_by_difftype_hard_mt<TContainer,
                                                        TColVal,
                                                        CORES,
                                                        Function,
                                                        SimdHash,
                                                        NPerGroup,
                                                        F>(x,
                                                           n_col,
                                                           colname,
                                                           f);
                    return;
                }
            } 
            transform_group_by_sametype_hard_mt<TContainer,
                                                TColVal,
                                                CORES, 
                                                Function,
                                                SimdHash,
                                                NPerGroup,
                                                F>(x,
                                                   n_col,
                                                   colname,
                                                   f);
        } else {
                transform_group_by_onecol_hard_mt<TContainer,
                                                  TColVal,
                                                  CORES,
                                                  Function,
                                                  SimdHash,
                                                  NPerGroup,
                                                  F>(x[0],
                                                     n_col,
                                                     colname,
                                                     f);
        }
    }

}


