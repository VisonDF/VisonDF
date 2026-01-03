#pragma once

struct MergeAlrdTriv {
    template <GroupFunction Function,
              [[unused]] unsigned int CORES>
    static void apply (
                       const size_t unique_grps,
                       const size_t chunks,
                       const std::vector<unsigned int>& grp_by,
                       const auto& val_grp_vec,
                       auto& val_grp
                       )
    {

        if constexpr (Function == GroupFunction::Occurence || 
                      Function == GroupFunction::Sum       ||
                      Function == GroupFunction::Mean) {

            for (size_t i = 0; i < unique_grps; ++i) {

                for (size_t i2 = 0; i2 < chunks; ++i2) {
                    const auto& cur_val = val_grp_vec[i2][i];
                    val_grp[grp_by[i]] += cur_val;
                }
            }

        } else {

            using TP = std:decay_t<decltype(val_grp[0].v)>::value_type;
            const unsigned int val_size = sizeof(TP);

            for (size_t i = 0; i < unique_grps; ++i) {

                for (size_t i2 = 0; i2 < chunks; ++i2) {
                    const auto& cur_val = val_grp_vec[i2][i];
                    const auto& cur_val_lookup = lookup[grp_by[i]];
                    const unsigned int n_old_size = cur_val_lookup.v.size();
                    cur_val_lookup.v.resize(n_old_size + cur_val.v.size());
                    memcpy(cur_val_lookup.v.data() + n_old_size,
                           cur_val.v.data(),
                           cur_val.v.size() * val_size
                           );
                }

            }

        }
    }
}


struct MergeAlrd {
    template <GroupFunction Function,
              [[unused]] unsigned int CORES>
    static void apply (
                       const size_t unique_grps,
                       const size_t chunks,
                       const std::vector<unsigned int>& grp_by,
                       const auto& val_grp_vec,
                       auto& val_grp
                       )
    {

        if constexpr (Function == GroupFunction::Occurence || 
                      Function == GroupFunction::Sum       ||
                      Function == GroupFunction::Mean) {

            for (size_t i = 0; i < unique_grps; ++i) {

                for (size_t i2 = 0; i2 < chunks; ++i2) {
                    const auto& cur_val = val_grp_vec[i2][i];
                    val_grp[grp_by[i]] += cur_val;
                }
            }

        } else {

            for (size_t i = 0; i < unique_grps; ++i) {

                for (size_t i2 = 0; i2 < chunks; ++i2) {
                    const auto& cur_val = val_grp_vec[i2][i];
                    const auto& cur_val_lookup = lookup[grp_by[i]];
                    cur_val_lookup.insert(cur_val_lookup.end(),
                                          cur_val.begin(),
                                          cur_val.end();
                }

            }

        }
    }
}



