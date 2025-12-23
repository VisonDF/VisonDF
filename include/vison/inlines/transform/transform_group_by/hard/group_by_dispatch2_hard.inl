#pragma once

template <typename TContainer = void
          typename TColVal,
          unsigned int CORES,
          GroupFunction Function,
          bool SimdHash,
          unsigned int NPerGroup,
          typename F
        >
inline group_by_dispatch2_hard(const std::vector<unsigned int>& x,
                               unsigned int ncol,
                               const std::string colname = "n",
                               const F f)
{ 
    if (x.size() > 1) {
        const char t_ref = type_refv[x[0]];
        for (auto& ii : x) {
            if (type_refv[ii] != t_ref) {
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
                return;
            }
        }
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





