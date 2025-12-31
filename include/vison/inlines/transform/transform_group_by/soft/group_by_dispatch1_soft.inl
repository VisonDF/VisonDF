#pragma once

template <typename TContainer,
          unsigned int CORES,
          bool SimdHash,
          bool MapCol,
          unsigned int NPerGroup
        >
inline group_by_dispatch1_soft(const std::vector<unsigned int>& x)
{ 
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




