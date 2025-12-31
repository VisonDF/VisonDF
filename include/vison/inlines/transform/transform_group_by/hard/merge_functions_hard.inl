#pragma once

struct MergeCurMapHard {
    template <GroupFunction Function>
    static void apply(const auto& zero_struct,
                      const auto& vec_struct,
                      const auto& cur_map,
                      auto& lookup) 
    {
        
        if constexpr (Function == GroupFunction::Occurence ||
                          Function == GroupFunction::Mean ||
                          Function == GroupFunction::Sum) {

            for (const auto& [k, v] : cur_map) {
    
                auto [it, inserted] = lookup.try_emplace(k, zero_struct);
                auto& cur_struct = it->second;
    
                cur_struct.value += v.value;
                const unsigned int n_old_size = cur_struct.idx_vec.size();
                cur_struct.idx_vec.resize(n_old_size + v.idx_vec.size());
    
                memcpy(cur_struct.idx_vec.data() + n_old_size,
                       v.idx_vec.data(),
                       v.idx_vec.size() * sizeof(unsigned int)
                       );

            }
    
        } else {

            for (const auto& [k, v] : cur_map) {
    
                auto [it, inserted] = lookup.try_emplace(k, vec_struct);
                auto& cur_struct = it->second;
    
                cur_struct.value.v.insert(cur_struct.value.v.end(),
                                          v.value.v.begin(),
                                          v.value.v.end());
    
                const unsigned int n_old_size = cur_struct.idx_vec.size();
                cur_struct.idx_vec.resize(n_old_size + v.idx_vec.size());
    
                memcpy(cur_struct.idx_vec.data() + n_old_size,
                       v.idx_vec.data(),
                       v.idx_vec.size() * sizeof(unsigned int)
                       );

            }
    
        }
    }
}

struct MergeCurMapTrivHard {
    template <GroupFunction Function>
    static void apply(const auto& zero_struct, 
                      const auto& vec_struct,
                      const auto& cur_map,
                      auto& lookup) 
    {
        
        if constexpr (Function == GroupFunction::Occurence ||
                          Function == GroupFunction::Mean ||
                          Function == GroupFunction::Sum) {

            for (const auto& [k, v] : cur_map) {
    
                auto [it, inserted] = lookup.try_emplace(k, zero_struct);
                auto& cur_struct = it->second;
    
                cur_struct.value += v.value;
                const unsigned int n_old_size = cur_struct.idx_vec.size();
                cur_struct.idx_vec.resize(n_old_size + v.idx_vec.size());
    
                memcpy(cur_struct.idx_vec.data() + n_old_size,
                       v.idx_vec.data(),
                       v.idx_vec.size() * sizeof(unsigned int)
                       );

            }
    
        } else {

            for (const auto& [k, v] : cur_map) {
    
                auto [it, inserted] = lookup.try_emplace(k, vec_struct);
                auto& cur_struct = it->second;
                const unsigned int n_old_size_val = cur_struct.value.v.size();
    
                cur_struct.value.v.resize(n_old_size_val + v.v.size());
                memcpy(cur_struct.value.v.data() + n_old_size_val,
                       v.value.v.data(),
                       v.value.v.size() * val_size);
    
                const unsigned int n_old_size = cur_struct.idx_vec.size();
                cur_struct.idx_vec.resize(n_old_size + v.idx_vec.size());
    
                memcpy(cur_struct.idx_vec.data() + n_old_size,
                       v.idx_vec.data(),
                       v.idx_vec.size() * sizeof(unsigned int)
                       );

            } 
        }
    }
}



