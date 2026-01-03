#pragma once

template <bool MapCol>
inline unsigned int idx_build_onecol(const unsigned int x, 
                                     const unsigned int idx_type)
{

    unsigned int real_pos;

    if constexpr (!MapCol) {

        auto it = std::find(matr_idx[idx_type].begin(), matr_idx[idx_type].end(), x);

        if (it != matr_idx[idx_type].end()) {

            n_col_real = std::distance(matr_idx[idx_type].begin(), it);
            break;

        } else {

            std::cerr << "`TColVal` type missmatch\n";
            return;

        }

    } else {

        if (matr_idx_map[idx_type].empty()) {
            std::cerr << "MapCol mode but no col found in matr_idx_map[idx_type]\n";
            return;
        }
        if (!sync_map_col[idx_type]) {
            std::cerr << "MapCol is not synced, consider applying `mapcol()`\n";
            return;
        }

        real_pos = matr_idx_map[idx_type][x];

    }
    return real_pos;
}


