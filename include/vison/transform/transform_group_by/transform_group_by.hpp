#pragma once

template <typename TContainer = void,
          typename TColVal = void,
          GroupFunction Function = GroupFunction::Occurence,
          bool SimdHash  = true,
          bool MapCol = false,
          unsigned int NPerGroup = 4,
          typename F = decltype(&default_groupfn_impl)>
requires Group<F, first_arg_grp_t<F>>
void transform_group_by(const std::vector<unsigned int>& x,
                        const n_col int,
                        const std::string colname = "n",
                        const F f = &default_groupfn_impl) 
{

    if constexpr (std::is_same_v<TColVal, void>) {
        switch(type_refv[n]) {
            case 's': group_by_dispatch1<TContainer, 
                                        std::string, 
                                        1, //CORES 
                                        Function, 
                                        SimdHash,
                                        MapCol,
                                        NPerGroup, 
                                        F>(x, ncol, colname, f); return;
            case 'c': group_by_dispatch1<TContainer, 
                                        CharT, 
                                        1, //CORES 
                                        Function, 
                                        SimdHash,
                                        MapCol,
                                        NPerGroup, 
                                        F>(x, ncol, colname, f); return;
            case 'b': group_by_dispatch1<TContainer, 
                                        uint8_t, 
                                        1, //CORES
                                        Function, 
                                        SimdHash,
                                        MapCol,
                                        NPerGroup, 
                                        F>(x, ncol, colname, f); return;
            case 'i': group_by_dispatch1<TContainer, 
                                        IntT, 
                                        1, //CORES 
                                        Function, 
                                        SimdHash, 
                                        MapCol,
                                        NPerGroup, 
                                        F>(x, ncol, colname, f); return;
            case 'u': group_by_dispatch1<TContainer, 
                                        UIntT, 
                                        1, //CORES
                                        Function, 
                                        SimdHash, 
                                        MapCol,
                                        NPerGroup, 
                                        F>(x, ncol, colname, f); return;
            case 'd': group_by_dispatch1<TContainer, 
                                        FloatT, 
                                        1, //CORES 
                                        Function, 
                                        SimdHash,
                                        MapCol,
                                        NPerGroup, 
                                        F>(x, ncol, colname, f); return;
        }
    } else {
        if (x.size() > 1) {
            const char t_ref = type_refv[x[0]];
            for (auto& ii : x) {
                if (type_refv[ii] != t_ref) {
                    transform_group_by_difftype_mt<TContainer,
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
            transform_group_by_sametype_mt<TContainer,
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
                transform_group_by_onecol_mt<TContainer,
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


