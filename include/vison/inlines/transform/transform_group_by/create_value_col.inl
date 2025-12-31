#pragma once

struct CreateValueCol {
    template <GroupFunction Function,
              unsigned int CORES>
    static void apply (auto&& f, 
                       auto& v_col,
                       auto& lookup,
                       const size_t local_nrow,
                       const auto& key_vec
                       )
    {

        #pragma omp parallel for if(CORES > 1) num_threads(CORES)
        for (size_t i = 0; i < key_vec.size(); ++i) {

            unsigned int count;

            if constexpr (Function == GroupFunction::Occurence || 
                          Function == GroupFunction::Sum) {

                count = lookup.at(*key_vec[i]);

            } else if constexpr (Function == GroupFunction::Mean) {

                count = lookup.at(*key_vec[i]) / local_nrow;

            } else if constexpr (Function == GroupFunction::Gather) {

                count = f(lookup.at(*key_vec[i]));

            }
            v_col[i] = count;
        }

    }
}
