#pragma once

struct MergeCurMap {
    template <GroupFunction Function>
    static void apply(const auto& zero,
                      const auto& vec,
                      const auto& cur_map,
                      auto& lookup) 
    {
        
        if constexpr (Function == GroupFunction::Occurence ||
                      Function == GroupFunction::Mean ||
                      Function == GroupFunction::Sum) {
        
            for (const auto& [k, v] : cur_map) {

                auto [it, inserted] = lookup.try_emplace(k, zero);
                it->second += v;

            }
        
        } else {
   
            for (const auto& [k, v] : cur_map) {

                auto [it, inserted] = lookup.try_emplace(k, vec);
        
                it->second.v.insert(it->second.v.end(),
                                    v.v.begin(),
                                    v.v.end());

            }
        
        }
    }
}

struct MergeCurMapTriv {
    template <GroupFunction Function>
    static void apply(const auto& zero, 
                      const auto& vec,
                      const auto& cur_map,
                      auto& lookup) 
    {
        
        if constexpr (Function == GroupFunction::Occurence ||
                      Function == GroupFunction::Mean ||
                      Function == GroupFunction::Sum) {
    
            for (const auto& [k, v] : cur_map) {

                auto [it, inserted] = lookup.try_emplace(k, zero);
                it->second += v;

            }
    
        } else {
  
            using value_t = typename std::decay_t<decltype(vec.v)>::value_type;
            const unsigned int val_size = sizeof(value_t);

            for (const auto& [k, v] : cur_map) {

                auto [it, inserted] = lookup.try_emplace(k, vec);
                const unsigned int n_old_size_val = it->second.v.size();
    
                it->second.v.resize(n_old_size_val + v.v.size());
                memcpy(it->second.v.data() + n_old_size_val,
                       v.v.data(),
                       v.v.size() * val_size);

            } 
        }
    }
}



