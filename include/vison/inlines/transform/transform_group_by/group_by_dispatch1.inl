#pragma once

template <typename TContainer = void
          typename TColVal,
          unsigned int CORES,
          GroupFunction Function,
          bool SimdHash,
          unsigned int NPerGroup,
          typename F
        >
inline group_by_dispatch1 (const std::vector<unsigned int>& x,
                           unsigned int ncol,
                           const std::string colname = "n",
                           const F f)
{ 
    if constexpr (std::is_same_v<TContainer, void>) {
        switch(type_refv[n]) {
            case 's': group_by_dispatch2<std::string, 
                                         TColVal, 
                                         CORES, 
                                         Function, 
                                         SimdHash, 
                                         NPerGroup, 
                                         F>(x, ncol, colname, f); return;
            case 'c': group_by_dispatch2<CharT, 
                                         TColVal, 
                                         CORES, 
                                         Function, 
                                         SimdHash, 
                                         NPerGroup, 
                                         F>(x, ncol, colname, f); return;
            case 'b': group_by_dispatch2<uint8_t, 
                                         TColVal, 
                                         CORES, 
                                         Function, 
                                         SimdHash, 
                                         NPerGroup, 
                                         F>(x, ncol, colname, f); return;
            case 'i': group_by_dispatch2<IntT, 
                                         TColVal, 
                                         CORES, 
                                         Function, 
                                         SimdHash, 
                                         NPerGroup, 
                                         F>(x, ncol, colname, f); return;
            case 'u': group_by_dispatch2<UIntT, 
                                         TColVal, 
                                         CORES, 
                                         Function, 
                                         SimdHash, 
                                         NPerGroup, 
                                         F>(x, ncol, colname, f); return;
            case 'd': group_by_dispatch2<FloatT, 
                                         TColVal, 
                                         CORES, 
                                         Function, 
                                         SimdHash, 
                                         NPerGroup, 
                                         F>(x, ncol, colname, f); return;
        }

    } else {
        if (x.size() > 1) {
            const char ref_t = type_refv[x[0]];
            for (auto& ii : x) {
                if (ref_t != type_refv[ii]) {
                    transform_group_by_difftype_mt<TContainer,
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
            transform_group_by_sametype_mt<TContainer,
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
                transform_group_by_onecol_mt<TContainer,
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


