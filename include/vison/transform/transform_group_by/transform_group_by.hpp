#pragma once

template <typename TContainer = void,
          typename TColVal = void,
          GroupFunction Function = GroupFunction::Occurence,
          bool SimdHash  = true,
          unsigned int NPerGroup = 4,
          typename F = decltype(&default_groupfn_impl)>
requires Group<F, first_arg_grp_t<F>>
void transform_group_by(const std::vector<unsigned int>& x,
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
            case 's': group_by_dispatch1<TContainer, 
                                        std::string, 
                                        1, 
                                        Function, 
                                        SimdHash, 
                                        NPerGroup, 
                                        F>(x, ncol, colname, f); break;
            case 'c': group_by_dispatch1<TContainer, 
                                        CharT, 
                                        1, 
                                        Function, 
                                        SimdHash, 
                                        NPerGroup, 
                                        F>(x, ncol, colname, f); break;
            case 'b': group_by_dispatch1<TContainer, 
                                        uint8_t, 
                                        1, 
                                        Function, 
                                        SimdHash, 
                                        NPerGroup, 
                                        F>(x, ncol, colname, f); break;
            case 'i': group_by_dispatch1<TContainer, 
                                        IntT, 
                                        1, 
                                        Function, 
                                        SimdHash, 
                                        NPerGroup, 
                                        F>(x, ncol, colname, f); break;
            case 'u': group_by_dispatch1<TContainer, 
                                        UIntT, 
                                        1, 
                                        Function, 
                                        SimdHash, 
                                        NPerGroup, 
                                        F>(x, ncol, colname, f); break;
            case 'd': group_by_dispatch1<TContainer, 
                                        FloatT, 
                                        1, 
                                        Function, 
                                        SimdHash, 
                                        NPerGroup, 
                                        F>(x, ncol, colname, f); break;
        }

    } else {
        if (x.size() > 1) {
            if constexpr (std::is_same_v<TContainer, TColVal>) {
                transform_group_by_sametype_mt<TContainer,
                                               TColVal,
                                               1, // CORES
                                               Function,
                                               SimdHash,
                                               NPerGroup,
                                               F>(x,
                                                  n_col,
                                                  colname,
                                                  f);
            } else {
                transform_group_by_difftype_mt<TContainer,
                                               TColVal,
                                               1, //CORES
                                               Function,
                                               SimdHash,
                                               NPerGroup,
                                               F>(x,
                                                  n_col,
                                                  colname,
                                                  f);
            }
        } else {
                transform_group_by_onecol_mt<TContainer,
                                             TColVal,
                                             1, //CORES
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


