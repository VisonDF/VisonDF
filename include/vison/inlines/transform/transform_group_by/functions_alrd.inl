#pragma once


inline void occ_grp_by(
                      const size_t start,
                      const size_t end,
                      [[unused]] const size_t val_idx,
                      const std::vector<unsigned int>& grp_by,
                      auto& vec_grp,
                      [[unused]] auto& val_table
                      )
{
    for (size_t i = start; i < end; ++i)
        ++vec_grp[grp_by[i]];
}

inline void add_grp_by(
                      const size_t start,
                      const size_t end,
                      const size_t val_idx,
                      const std::vector<unsigned int>& grp_by,
                      auto& vec_grp,
                      auto& val_table
                      )
{
    for (size_t i = start; i < end; ++i)
        vec_grp[grp_by[i]] += val_table[val_idx];
}

inline void fill_grp_by(
                       const size_t start,
                       const size_t end,
                       const size_t val_idx,
                       const std::vector<unsigned int>& grp_by,
                       auto& vec_grp,
                       auto& val_table
                       )
{
    for (size_t i = start; i < end; ++i)
        vec_grp[grp_by[i]].v.push_back(val_table[val_idx]);
}






