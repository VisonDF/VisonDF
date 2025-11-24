#pragma once

void reorder_col(const std::vector<std::pair<unsigned int, unsigned int>>& swaps)
{
    ankerl::unordered_dense::map<unsigned int, std::pair<size_t, size_t>> pos_of;
    pos_of.reserve(ncol);

    for (size_t i = 0; i < matr_idx.size(); ++i) {
        for (size_t j = 0; j < matr_idx[i].size(); ++j) {
            pos_of[matr_idx[i][j]] = {i, j};
        }
    }

    for (auto& [old_pos, new_pos] : swaps) {

        assert(old_pos < ncol && "old_pos out of bounds");
        assert(new_pos < ncol && "new_pos out of bounds");

        std::swap(type_refv[old_pos], type_refv[new_pos]);
        std::swap(tmp_val_refv[old_pos], tmp_val_refv[new_pos]);

        if (!name_v.empty()) {
            std::swap(name_v[old_pos], name_v[new_pos]);
        }

        auto [r1, c1] = pos_of[old_pos];
        auto [r2, c2] = pos_of[new_pos];

        std::swap(matr_idx[r1][c1], matr_idx[r2][c2]);

        pos_of[old_pos] = {r2, c2};
        pos_of[new_pos] = {r1, c1};
    }
}



