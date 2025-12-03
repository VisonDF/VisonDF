#pragma once

void reorder_col(const std::vector<std::pair<unsigned int, unsigned int>>& swaps)
{

    ankerl::unordered_dense::set<unsigned> seen;
    seen.reserve(swaps.size() * 2);

    for (auto& p : swaps) {
        auto a = p.first;
        auto b = p.second;

        if (!seen.insert(a).second || !seen.insert(b).second) {
            std::cerr << "Cols must be unique among all swap pairs\n";
            return;
        }
    }

    ankerl::unordered_dense::map<unsigned, std::pair<size_t, size_t>> pos_of;
    pos_of.reserve(ncol);

    for (size_t i = 0; i < matr_idx.size(); ++i)
        for (size_t j = 0; j < matr_idx[i].size(); ++j)
            pos_of[matr_idx[i][j]] = {i, j};

    for (int k = 0; k < (int)swaps.size(); ++k) {

        unsigned old_pos = swaps[k].first;
        unsigned new_pos = swaps[k].second;

        assert(old_pos < ncol && "old_pos out of bounds");
        assert(new_pos < ncol && "new_pos out of bounds");

        std::swap(type_refv[old_pos], type_refv[new_pos]);
        std::swap(tmp_val_refv[old_pos], tmp_val_refv[new_pos]);

        if (!name_v.empty())
            std::swap(name_v[old_pos], name_v[new_pos]);

        auto loc1 = pos_of[old_pos];
        auto loc2 = pos_of[new_pos];

        std::swap(matr_idx[loc1.first][loc1.second],
                  matr_idx[loc2.first][loc2.second]);
    }

}

