#pragma once

inline void grp_by_alrd_soft (
                             const size_t start,
                             const size_t end,
                             const std::vector<unsigned int>& grp_by,
                             auto& val_grp
                             )
{
    for (size_t i = start; i < end; ++i)
        val_grp[grp_by[i]].v.push_back(i);
}






