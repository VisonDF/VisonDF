#pragma once

struct CreateValueCol {
    template <GroupFunction Function,
              unsigned int CORES,
              bool StandardMethod = true>
    static void apply (auto&& f, 
                       auto& v_col,
                       auto& lookup,
                       const size_t local_nrow,
                       const auto& key_vec
                       )
    {

        #pragma omp parallel for if(CORES > 1) num_threads(CORES)
        for (size_t i = 0; i < key_vec.size(); ++i) {

            if constexpr (Function == GroupFunction::Occurence || 
                          Function == GroupFunction::Sum) {

                v_col[i] = lookup.at(*key_vec[i]);

            } else if constexpr (Function == GroupFunction::Mean) {

                if constexpr (StandardMethod) {
                    for (auto& [k, v] : lookup)
                        v / local_nrow;
                }

                v_col[i] = lookup.at(*key_vec[i]) / local_nrow;

            } else if constexpr (Function == GroupFunction::Gather) {

                if constexpr (StandardM%ethod) {

                    v_col[i] = f(lookup.at(*key_vec[i]).v);

                } else {

                    using key_t = typename lookup::key_type;
                    using val_t = std::decay_t<decltype(v_col)>::value_type;
                    ankerl::unordered_dense::map<key_t, val_t> lookup2;

                    lookup2.reserve(lookup.size());
                    for (auto& [k, v] : lookup)
                        lookup2.emplace(k, f(v.v));

                    v_col[i] = lookup2.at(*key_vec[i]);

                }

            }
        }

    }
}


