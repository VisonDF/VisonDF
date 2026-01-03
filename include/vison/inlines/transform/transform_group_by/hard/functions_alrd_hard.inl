#pragma once


inline void occ_grp_by_hard(
                            const size_t start,
                            const size_t end,
                            [[unused]] const size_t val_idx,
                            const std::vector<unsigned int>& grp_by,
                            auto& vec_grp,
                            [[unused]] auto& val_table
                            )
{
    for (size_t i = start; i < end; ++i) {
        auto& cur_pair = vec_grp[grp_by[i]];
        ++cur_pair.value;
        cur_pair.idx_vec.v.push_back(i);
    }
}

inline void add_grp_by_hard(
                            const size_t start,
                            const size_t end,
                            const size_t val_idx,
                            const std::vector<unsigned int>& grp_by,
                            auto& vec_grp,
                            auto& val_table
                            )
{
    for (size_t i = start; i < end; ++i) {
        auto& cur_pair = vec_grp[grp_by[i]];
        cur_pair.value += val_table[val_idx];
        cur_pair.idx_vec.v.push_back(i);
    }
}

inline void fill_grp_by_hard(
                             const size_t start,
                             const size_t end,
                             const size_t val_idx,
                             const std::vector<unsigned int>& grp_by,
                             auto& vec_grp,
                             auto& val_table
                             )
{
    for (size_t i = start; i < end; ++i) {
        auto& cur_pair = vec_grp[grp_by[i]];
        cur_pair.value.v.push_back(val_table[val_idx]);
        cur_pair.idx_vec.v.push_back(i);
    }
}







