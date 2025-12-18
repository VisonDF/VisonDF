#pragma once

template <typename TContainer = void
          typename TColVal,
          unsigned int CORES,
          GroupFunction Function,
          bool SimdHash,
          unsigned int NPerGroup,
          typename F
        >
inline group_by_dispatch1_hard (const std::vector<unsigned int>& x,
                           	unsigned int ncol,
                           	const std::string colname = "n",
                           	const F f)
{ 
    if constexpr (std::is_same_v<TContainer, void>) {
        switch(type_refv[n]) {
            case 's': group_by_dispatch2_hard<std::string, 
                                              TColVal, 
                                              CORES, 
                                              Function, 
                                              SimdHash, 
                                              NPerGroup, 
                                              F>(x, ncol, colname, f); break;
            case 'c': group_by_dispatch2_hard<CharT, 
                                              TColVal, 
                                              CORES, 
                                              Function, 
                                              SimdHash, 
                                              NPerGroup, 
                                              F>(x, ncol, colname, f); break;
            case 'b': group_by_dispatch2_hard<uint8_t, 
                                              TColVal, 
                                              CORES, 
                                              Function, 
                                              SimdHash, 
                                              NPerGroup, 
                                              F>(x, ncol, colname, f); break;
            case 'i': group_by_dispatch2_hard<IntT, 
                                              TColVal, 
                                              CORES, 
                                              Function, 
                                              SimdHash, 
                                              NPerGroup, 
                                              F>(x, ncol, colname, f); break;
            case 'u': group_by_dispatch2_hard<UIntT, 
                                              TColVal, 
                                              CORES, 
                                              Function, 
                                              SimdHash, 
                                              NPerGroup, 
                                              F>(x, ncol, colname, f); break;
            case 'd': group_by_dispatch2_hard<FloatT, 
                                              TColVal, 
                                              CORES, 
                                              Function, 
                                              SimdHash, 
                                              NPerGroup, 
                                              F>(x, ncol, colname, f); break;
        }

    } else {
        if (x.size() > 1) {
            if (std::is_same_v<TContainer, TColVal>) {
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
            }
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


