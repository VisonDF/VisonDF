#pragma once

template <bool IsBool = false,
          bool MapCol = false,
          typename T
         >
void get_col_filter_idx_simd(const unsigned int x, 
                             std::vector<T> &rtn_v,
                             const std::vector<unsigned int> &mask) {
  
    const unsigned int n_el = mask.size();
    rtn_v.resize(n_el);

    auto find_col_base = [x](const auto &idx_vec, [[maybe_unused]] const size_t idx_type) -> size_t {
        size_t pos;
        if constexpr (!MapCol) {
            pos = 0;
            while (pos < idx_vec.size() && idx_vec[pos] != x)
                ++pos;
  
            if (pos == idx_vec.size())
                throw std::runtime_error("Error in (get_col), no column found\n");
        } else {
            if (!matr_idx_map[idx_type].contains(x)) {
                throw std::runtime_error("MapCol chosen but col not found in map\n");
            }
            if (!sync_map_col[idx_type]) {
                throw std::runtime_error("Map not synced\n");
            }
            pos = matr_idx_map[idx_type][x];
        }
        return pos;
    };

    if constexpr (std::is_same_v<T, std::string>) {

        const size_t pos_base = find_col_base(matr_idx[0], 0);
        const std::vector<std::string>& src = str_v[pos_base];
        for (size_t i = 0; i < n_el; ++i)
            rtn_v[i] = src[mask[i]];

    } else if constexpr (std::is_same_v<T, CharT>) {

        const size_t pos_base = find_col_base(matr_idx[1], 1);
        const std::vector<CharT>& src = chr_v[pos_base];
        for (size_t i = 0; i < n_el; ++i)
            memcpy(rtn_v[i].data(),
                   src[mask[i]].data(),
                   sizeof(CharT));

    } else if constexpr (IsBool) { 

        const size_t pos_base = find_col_base(matr_idx[2], 2);
        get_filtered_col_idx_8<T>(bool_v[pos_base]);

    } else if constexpr (std::is_same_v<T, IntT>) {

        const size_t pos_base = find_col_base(matr_idx[3], 3);
        if constexpr (sizeof(T) == 1) {
            get_filtered_col_idx_8<T>      (int_v[pos_base],
                                            rtn_v,
                                            mask);
        } else if constexpr (sizeof(T) == 2) {
            get_filtered_col_idx_16<T>     (int_v[pos_base],
                                            rtn_v,
                                            mask);        
        } else if constexpr (sizeof(T) == 4)  {
            get_filtered_col_idx_32<T>     (int_v[pos_base],
                                            rtn_v,
                                            mask);
        } else if constexpr (sizeof(T) == 8) {
            get_filtered_col_idx_64<T>     (int_v[pos_base],
                                            rtn_v,
                                            mask);
        } else {
            get_filtered_col_idx_general<T>(int_v[pos_base],
                                            rtn_v,
                                            mask);
        }

    } else if constexpr (std::is_same_v<T, UIntT>) {

        const size_t pos_base = find_col_base(matr_idx[4], 4);
        if constexpr (sizeof(T) == 1) {
            get_filtered_col_idx_8<T>     (uint_v[pos_base],
                                            rtn_v,
                                            mask);
        } else if constexpr (sizeof(T) == 2) {
            get_filtered_col_idx_16<T>     (uint_v[pos_base],
                                            rtn_v,
                                            mask);        
        } else if constexpr (sizeof(T) == 4)  {
            get_filtered_col_idx_32<T>     (uint_v[pos_base],
                                            rtn_v,
                                            mask);
        } else if constexpr (sizeof(T) == 8) {
            get_filtered_col_idx_64<T>     (uint_v[pos_base],
                                            rtn_v,
                                            mask);
        } else {
            get_filtered_col_idx_general<T>(uint_v[pos_base],
                                            rtn_v,
                                            mask);
        }

    } else if constexpr (std::is_same_v<T, FloatT>) {

        const size_t pos_base = find_col_base(matr_idx[5], 5);
        std::vector<FloatT>& src = dbl_v[i2];

        if constexpr (sizeof(T) == 8) {
             get_filtered_col_idx_64<T>    (uint_v[pos_base],
                                            rtn_v,
                                            mask);          
        } else {
             get_filtered_col_idx_32<T>    (uint_v[pos_base],
                                            rtn_v,
                                            mask);
        }

    } else {

      throw std::runtime_error("Error in (get_col), unsupported type\n");

    };

};








