#pragma once

template <typename TContainer = void,
          unsigned int CORES = 4,
          bool SimdHash  = true,
          bool MapCol = false,
          unsigned int NPerGroup = 4>
requires Group<F, first_arg_grp_t<F>>
void transform_group_by_soft_mt(const std::vector<unsigned int>& x)
{

    if constexpr (std::is_same_v<TContainer, void>) {

        switch(type_refv[n]) {
            case 's': group_by_dispatch1_soft<std::string, 
                                              CORES,
                                              SimdHash,
                                              MapCol,
                                              NPerGroup>(x); break;
            case 'c': group_by_dispatch1_soft<CharT, 
                                              CORES,
                                              SimdHash, 
                                              MapCol,
                                              NPerGroup>(x); break;
            case 'b': group_by_dispatch1_soft<uint8_t, 
                                              CORES,
                                              SimdHash, 
                                              MapCol,
                                              NPerGroup>(x); break;
            case 'i': group_by_dispatch1_soft<IntT, 
                                              CORES,
                                              SimdHash, 
                                              MapCol,
                                              NPerGroup>(x); break;
            case 'u': group_by_dispatch1_soft<UIntT, 
                                              CORES,
                                              SimdHash, 
                                              MapCol,
                                              NPerGroup>(x); break;
            case 'd': group_by_dispatch1_soft<FloatT, 
                                              CORES,
                                              SimdHash, 
                                              MapCol,
                                              NPerGroup>(x); break;
        }

    } else {
        if (x.size() > 1) {
            const char t_ref = type_refv[x[0]];
            for (auto& ii : x) {
                if (type_refv[ii] != t_ref) {
                    transform_group_by_difftype_soft_mt<TContainer,
                                                        CORES,
                                                        SimdHash,
                                                        MapCol,
                                                        NPerGroup>(x);
                    return;
                }
            }
            transform_group_by_sametype_soft_mt<TContainer,
                                                CORES,
                                                SimdHash,
                                                MapCol,
                                                NPerGroup>(x);
        } else {
                transform_group_by_onecol_soft_mt<TContainer,
                                                  CORES,
                                                  SimdHash,
                                                  MapCol,
                                                  NPerGroup>(x[0]);
        }
    }

}





