#pragma once

struct MergeAlrd {
    template <GroupFunction Function,
              unsigned int CORES>
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

    	    #pragma omp parallel for num_threads(CORES)
            for (size_t i = 0; i < unique_grps; ++i) {

                for (size_t i2 = 0; i2 < chunks; ++i2) {
                    const auto& cur_val = val_grp_vec[i2][i];
                    val_grp[grp_by[i]] += cur_val;
                }
            }

        } else {

    	    #pragma omp parallel for num_threads(CORES)
            for (size_t i = 0; i < unique_grps; ++i) {

                for (size_t i2 = 0; i2 < chunks; ++i2) {
                    const auto& cur_val = val_grp_vec[i2][i];
                    val_grp[grp_by[i]].v.push_back(cur_val);
                }

            }

        }
    }
}

