#pragma once

struct MergeAlrdTrivHard {
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

    	    #pragma omp parallel for if(CORES > 1) num_threads(CORES)
            for (size_t i = 0; i < unique_grps; ++i) {

                auto& val_lookup = val_grp[i];

                for (size_t i2 = 0; i2 < chunks; ++i2) {
                    const auto& cur_val = val_grp_vec[i2][i];
                    val_lookup.value += cur_val;
                    const unsigned int n_old_size = val_lookup.idx_vec.v.size();
                    val_lookup.idx_vec.v.resize(n_old_size + cur_val.idx_vec.v.size());
                    memcpy(val_lookup.idx_vec.v.data() + n_old_size,
                           cur_val.idx_vec.v.data(),
                           cur_val.idx_vec.v.size() * sizeof(unsigned int));
                }
            }

        } else {

            using TP = std:decay_t<decltype(val_grp[0].value.v)>::value_type;
            const unsigned int val_size = sizeof(TP);

    	    #pragma omp parallel for if(CORES > 1) num_threads(CORES)
            for (size_t i = 0; i < unique_grps; ++i) {

                auto& val_lookup = val_grp[i];

                for (size_t i2 = 0; i2 < chunks; ++i2) {
                    const auto& cur_val = val_grp_vec[i2][i];
                    const unsigned int n_old_size2 = val_lookup.value.v.size();
                    val_lookup.value.v.resize(n_old_size2 + cur_val.value.v.size());
                    memcpy(val_lookup.value.v.data() + n_old_size2,
                           cur_val.value.v.data(),
                           cur_val.value.v.size() * val_size);
                    const unsigned int n_old_size = val_lookup.idx_vec.v.size();
                    val_lookup.idx_vec.v.resize(n_old_size + cur_val.idx_vec.v.size());
                    memcpy(val_lookup.idx_vec.v.data() + n_old_size,
                           cur_val.idx_vec.v.data(),
                           cur_val.idx_vec.v.size() * sizeof(unsigned int));
                }

            }

        }
    }
}

struct MergeAlrdHard {
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

    	    #pragma omp parallel for if(CORES > 1) num_threads(CORES)
            for (size_t i = 0; i < unique_grps; ++i) {

                auto& val_lookup = val_grp[i];

                for (size_t i2 = 0; i2 < chunks; ++i2) {
                    const auto& cur_val = val_grp_vec[i2][i];
                    val_lookup.value += cur_val;
                    const unsigned int n_old_size = val_lookup.idx_vec.v.size();
                    val_lookup.idx_vec.v.resize(n_old_size + cur_val.idx_vec.v.size());
                    memcpy(val_lookup.idx_vec.v.data() + n_old_size,
                           cur_val.idx_vec.v.data(),
                           cur_val.idx_vec.v.size() * sizeof(unsigned int));
                }
            }

        } else {

    	    #pragma omp parallel for if(CORES > 1) num_threads(CORES)
            for (size_t i = 0; i < unique_grps; ++i) {

                auto& val_lookup = val_grp[i];

                for (size_t i2 = 0; i2 < chunks; ++i2) {
                    const auto& cur_val  = val_grp_vec[i2][i];
                    val_lookup.v.insert(
                                        val_lookup.v.end(),
                                        cur_val.v.begin(),
                                        cur_val.v.end()
                                        );
                    const unsigned int n_old_size = val_lookup.idx_vec.v.size();
                    val_lookup.idx_vec.v.resize(n_old_size + cur_val.idx_vec.v.size());
                    memcpy(val_lookup.idx_vec.v.data() + n_old_size,
                           cur_val.idx_vec.v.data(),
                           cur_val.idx_vec.v.size() * sizeof(unsigned int));
                }

            }

        }
    }
}



