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
    if (x.size() > 1) {
        if (std::is_same_v<TContainer, TColVal>) {
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
        }
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


