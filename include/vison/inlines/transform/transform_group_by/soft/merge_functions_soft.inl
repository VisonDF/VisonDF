#pragma once

template <unsigned int NPerGroup>
void merge_soft(const auto& vec_map,
                auto& lookup)
{

    const ReservingVec<unsigned int> vec(NPerGroup);

    for (const auto& cur_map : vec_map) {

        for (const auto& [k, v] : cur_map) {

            auto [it, inserted] = lookup.try_emplace(k, vec);
            const unsigned int n_old_size = it->second.v.size();

            it->second.v.resize(n_old_size + v.v.size());
            memcpy(it->second.v.data() + n_old_size,
                   v.v.data(),
                   v.v.size() * sizeof(unsigned int)
                   );

        }
    }
}


