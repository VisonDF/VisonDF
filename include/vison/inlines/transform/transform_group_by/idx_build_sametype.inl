#pragma once

template <bool MapCol>
inline std::vector<unsigned int> idx_build_sametype(std::vector<unsigned int>& x,
                                                    const unsigned int type_idx)
{

    std::vector<unsigned int> idx;
    idx.reserve(x.size());

    if constexpr (!MapCol) {

        std::unordered_map<unsigned int, unsigned int> pos;
        const auto& cur_matr_idx = matr_idx[idx_type];

        for (int i = 0; i < matr_idx[idx_type].size(); ++i)
            pos[cur_matr_idx[i]] = i;

        for (int v : x)
            idx.push_back(pos[v]);

    } else {

        const auto& cur_col_map = matr_idx_map[idx_type];

        if (cur_col_map.empty()) {
            std::cerr << "MapCol mode but no col found in matr_idx_map[idx_type]\n";
            return;
        }

        for (int v : x)
            idx.push_back(cur_col_map[v]);

    }

    std::sort(idx.begin(), idx.end());
    return idx;
}


