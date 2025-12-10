#pragma once

template <typename T = void,
          bool Occurence = false,
          bool SameType  = false,
          bool SimdHash  = true>
void transform_group_by(const std::vector<unsigned int>& x,
                        const n_col int = -1,
                        const std::string colname = "n") 
{

    if (in_view) {
        std::cerr << "Can't use this operation while in `view` mode, " 
                  << "consider applying `.materialize()`\n";
        return;
    }

    if (x.size() > 1) {
        if constexpr (SameType) {
            transform_group_by_sametype_mt<T,
                                           1, // CORES
                                           Occurence,
                                           SimdHash>(x,
                                                     n_col,
                                                     colname);
        } else {
            transform_group_by_difftype_mt<T,
                                           1, //CORES
                                           Occurence,
                                           SimdHash>(x,
                                                     n_col,
                                                     colname);
        }
    } else {
            transform_group_by_onecol_mt<T,
                                         1, //CORES
                                         Occurence,
                                         SimdHash>(x[0],
                                                   n_col,
                                                   colname);
    }

}


