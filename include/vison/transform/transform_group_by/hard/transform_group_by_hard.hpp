#pragma once

template <typename TContainer = void,
          typename TColVal = void,
          GroupFunction Function = GroupFunction::Occurence,
          bool SimdHash  = true,
          bool MapCol = false,
          unsigned int NPerGroup = 4,
          typename F = decltype(&default_groupfn_impl)>
requires Group<F, first_arg_grp_t<F>>
void transform_group_by_hard(const std::vector<unsigned int>& x,
                             const n_col int,
                             const std::string colname = "n",
                             const F f = &default_groupfn_impl) 
{

    if constexpr (std::is_same_v<TColVal, void>) {

        switch(type_refv[n]) {
            case 's': group_by_dispatch1_hard<TContainer, 
                                              std::string, 
                                              1, //CORES
                                              Function, 
                                              SimdHash,
                                              MapCol,
                                              NPerGroup, 
                                              F>(x, ncol, colname, f); break;
            case 'c': group_by_dispatch1_hard<TContainer, 
                                              CharT, 
                                              1, //CORES 
                                              Function, 
                                              SimdHash,
                                              MapCol,
                                              NPerGroup, 
                                              F>(x, ncol, colname, f); break;
            case 'b': group_by_dispatch1_hard<TContainer, 
                                              uint8_t, 
                                              1, //CORES 
                                              Function, 
                                              SimdHash,
                                              MapCol,
                                              NPerGroup, 
                                              F>(x, ncol, colname, f); break;
            case 'i': group_by_dispatch1_hard<TContainer, 
                                              IntT, 
                                              1, //CORES
                                              Function, 
                                              SimdHash,
                                              MapCo,
                                              NPerGroup, 
                                              F>(x, ncol, colname, f); break;
            case 'u': group_by_dispatch1_hard<TContainer, 
                                              UIntT, 
                                              1, //CORES
                                              Function, 
                                              SimdHash,
                                              MapCol,
                                              NPerGroup, 
                                              F>(x, ncol, colname, f); break;
            case 'd': group_by_dispatch1_hard<TContainer, 
                                              FloatT, 
                                              1, //CORES
                                              Function, 
                                              SimdHash,
                                              MapCol,
                                              NPerGroup, 
                                              F>(x, ncol, colname, f); break;
        }

    } else {
        if (x.size() > 1) {
            const char t_ref = type_refv[x[0]];
            for (auto& ii : x) {
                if (type_refv[ii] != t_ref) {
                    transform_group_by_difftype_hard_mt<TContainer,
                                                        TColVal,
                                                        1, //CORES
                                                        Function,
                                                        SimdHash,
                                                        MapCol,
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
                                                1, //CORES
                                                Function,
                                                SimdHash,
                                                MapCol,
                                                NPerGroup,
                                                F>(x,
                                                   n_col,
                                                   colname,
                                                   f);
        } else {
            transform_group_by_onecol_hard_mt<TContainer,
                                              TColVal,
                                              1, //CORES
                                              Function,
                                              SimdHash,
                                              MapCol,
                                              NPerGroup,
                                              F>(x[0],
                                                 n_col,
                                                 colname,
                                                 f);
        }
    }

}


