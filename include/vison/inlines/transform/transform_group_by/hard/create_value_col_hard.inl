#pragma once

struct CreateValueColHard {
    template <GroupFunction Function,
              unsigned int CORES = 4,
              [[unused]] bool StandardMethod>
    static void apply(auto&& f, 
                      auto& value_col,
                      const auto& cur_map,
                      const size_t local_nrow,
                      [[unused]] const auto& key_vec
                      )
    {
    
        if constexpr (CORES > 1) {
    
            std::vector<size_t> pos_boundaries;
            pos_boundaries.reserve(cur_map.size() + 1);
            pos_boundaries.push_back(0);
            
            for (auto it = cur_map.begin(); it != cur_map.end(); ++it) {
                pos_boundaries.push_back(
                    pos_boundaries.back() + it->second.idx_vec.size()
                );
            }
            
            auto it0 = cur_map.begin();
            
            #pragma omp parallel for num_threads(CORES) schedule(static)
            for (size_t g = 0; g < cur_map.size(); ++g) {
    
                size_t start = pos_boundaries[g];
                size_t len   = pos_boundaries[g + 1] - pos_boundaries[g];
    
                const std::vector<unsigned int>& vec = (it0 + g)->second.idx_vec.v;
                const auto cur_val     = (it0 + g)->second.value;
    
                if constexpr (Function == GroupFunction::Occurence ||
                              Function == GroupFunction::Sum) {
    
                    auto* __restrict__ out = value_col.data() + start;
                    const auto* __restrict__ in = &cur_val;
                    std::fill_n(out, len, *in);
    
                } else if constexpr (Function == GroupFunction::Mean) {
    
                    const auto cur_val2 = cur_val / local_nrow;
                    auto* __restrict__ out = value_col.data() + start;
                    const auto* __restrict__ in = &cur_val2;
                    std::fill_n(out, len, *in);


                } else {
    
                    const auto cur_val2 = f(cur_val.v);
                    auto* __restrict__ out = value_col.data() + start;
                    const auto* __restrict__ in = &cur_val2;
                    std::fill_n(out, len, *in);
    
                }
    
                memcpy(row_view_idx.data() + start,
                       vec.data(),
                       len * sizeof(unsigned int));
    
            }
        } else {
    
            auto it = cur_map.begin();
            size_t i2 = 0;
    
            for (size_t i = 0; i < cur_map.size(); ++i) {
    
                const std::vector<unsigned int>& vec = (it + i)->second.idx_vec.v;
                const auto cur_val = (it + i)->second.value;
    
                if constexpr (Function == GroupFunction::Occurence ||
                              Function == GroupFunction::Sum) {
    
                    auto* __restrict__ out = value_col.data() + start;
                    const auto* __restrict__ in = &cur_val;
                    std::fill_n(out, len, *in);
    
                } else if constexpr (Function == GroupFunction::Mean) {
    
                    const auto cur_val2 = cur_val / local_nrow;
                    auto* __restrict__ out = value_col.data() + start;
                    const auto* __restrict__ in = &cur_val2;
                    std::fill_n(out, len, *in);
    
                } else {
    
                    const auto cur_val2 = f(cur_val);
                    auto* __restrict__ out = value_col.data() + start;
                    const auto* __restrict__ in = &cur_val2;
                    std::fill_n(out, len, *in);
    
                }
    
                memcpy(row_view_idx.data() + i2, 
                       vec.data(), 
                       sizeof(unsigned int) * vec.size());
    
                i2 += vec.size();
            }
        }
    
    }
}


