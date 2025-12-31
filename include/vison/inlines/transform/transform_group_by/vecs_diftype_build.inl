#pragma once

template <bool MapCol>
inline void vecs_difftype_build (const std::vector<unsigned int>& x,
                                 const std::vector<unsigned int>& idx_str,
                                 const std::vector<unsigned int>& idx_chr,
                                 const std::vector<unsigned int>& idx_bool,
                                 const std::vector<unsigned int>& idx_int,
                                 const std::vector<unsigned int>& idx_uint,
                                 const std::vector<unsigned int>& idx_dbl,
                                 const size_t local_ncol
                                 )
{

    const rsv_val = local_ncol / 2;

    idx_str .reserve(rsv_val);
    idx_chr .reserve(rsv_val);
    idx_bool.reserve(rsv_val);
    idx_int .reserve(rsv_val);
    idx_uint.reserve(rsv_val);
    idx_dbl .reserve(rsv_val);

    if constexpr (!MapCol) {
        {
            const auto& cur_matr_idx = matr_idx[0];
            for (int v : x) {
                auto it = std::find(cur_matr_idx.begin(), cur_matr_idx.end(), v);
                if (it != cur_matr_idx.end())
                    idx_str.push_back(std::distance(cur_matr_idx.begin(), it));
            }
        }
        {
            const auto& cur_matr_idx = matr_idx[1];
            for (int v : x) {
                auto it = std::find(cur_matr_idx.begin(), cur_matr_idx.end(), v);
                if (it != cur_matr_idx.end())
                    idx_chr.push_back(std::distance(cur_matr_idx.begin(), it));
            }
        }
        {
            const auto& cur_matr_idx = matr_idx[2];
            for (int v : x) {
                auto it = std::find(cur_matr_idx.begin(), cur_matr_idx.end(), v);
                if (it != cur_matr_idx.end())
                    idx_bool.push_back(std::distance(cur_matr_idx.begin(), it));
            }
        }
        {
            const auto& cur_matr_idx = matr_idx[3];
            for (int v : x) {
                auto it = std::find(cur_matr_idx.begin(), cur_matr_idx.end(), v);
                if (it != cur_matr_idx.end())
                    idx_int.push_back(std::distance(cur_matr_idx.begin(), it));
            }
        }
        {
            const auto& cur_matr_idx = matr_idx[4];
            for (int v : x) {
                auto it = std::find(cur_matr_idx.begin(), cur_matr_idx.end(), v);
                if (it != cur_matr_idx.end())
                    idx_uint.push_back(std::distance(cur_matr_idx.begin(), it));
            }
        }
        {
            const auto& cur_matr_idx = matr_idx[5];
            for (int v : x) {
                auto it = std::find(cur_matr_idx.begin(), cur_matr_idx.end(), v);
                if (it != cur_matr_idx.end())
                    idx_dbl.push_back(std::distance(cur_matr_idx.begin(), it));
            }
        }
    } else {
        {
            const auto& cur_col_map = matr_idx_map[0];
            for (int v : x) {
                if (cur_col_map.contains(v))
                    idx_str.push_back(cur_matr_idx_map[v])
            }
        }
        {
            const auto& cur_col_map = matr_idx_map[1];
            for (int v : x) {
                if (cur_col_map.contains(v))
                    idx_chr.push_back(cur_matr_idx_map[v])
            }
        }
        {
            const auto& cur_col_map = matr_idx_map[2];
            for (int v : x) {
                if (cur_col_map.contains(v))
                    idx_bool.push_back(cur_matr_idx_map[v])
            }
        }
        {
            const auto& cur_col_map = matr_idx_map[3];
            for (int v : x) {
                if (cur_col_map.contains(v))
                    idx_int.push_back(cur_matr_idx_map[v])
            }
        }
        {
            const auto& cur_col_map = matr_idx_map[4];
            for (int v : x) {
                if (cur_col_map.contains(v))
                    idx_uint.push_back(cur_matr_idx_map[v])
            }
        }
        {
            const auto& cur_col_map = matr_idx_map[5];
            for (int v : x) {
                if (cur_col_map.contains(v))
                    idx_dbl.push_back(cur_matr_idx_map[v])
            }
        }
    }

    std::sort(idx_str.begin(),  idx_str.end());
    std::sort(idx_chr.begin(),  idx_chr.end());
    std::sort(idx_bool.begin(), idx_bool.end());
    std::sort(idx_int.begin(),  idx_int.end());
    std::sort(idx_uint.begin(), idx_uint.end());
    std::sort(idx_dbl.begin(),  idx_dbl.end());

}



