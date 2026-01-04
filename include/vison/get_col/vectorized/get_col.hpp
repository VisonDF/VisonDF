#pragma once

template <bool IsBool = false,
          bool MapCol = false,
          typename T>
void get_col(unsigned int &x, 
             std::vector<T> &rtn_v) {
 
    const unsigned int local_nrow = nrow;
    rtn_v.resize(local_nrow);

    auto find_col_base = [x](const auto &idx_vec, [[maybe_unused]] const size_t idx_type) -> size_t {
        size_t pos;
        if constexpr (!MapCol) {
            pos = 0;
            while (pos < idx_vec.size() && idx_vec[pos] != x)
                ++pos;
            if (pos == idx_vec.size()) {
                throw std::runtime_error("Error in (get_col), no column found\n");
            }
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

    auto load_column = [&rtn_v](const auto*__restrict src)
    {
        memcpy(rtn_v.data(), 
               src, 
               local_nrow * sizeof(T));
    };

    if constexpr (std::is_same_v<T, std::string>) {

      const size_t pos_base = find_col_base(matr_idx[0], 0);
      const auto& src = str_v[pos_base];
      for (size_t i = 0; i < local_nrow; ++i)
          rtn_v[i] = src[i];

    } else if constexpr (std::is_same_v<T, CharT>) {

      const size_t pos_base = find_col_base(matr_idx[1], 1);
      load_column(chr_v[pos_base].data());

    } else if constexpr (IsBool) {

      const size_t pos_base = find_col_base(matr_idx[2], 2);
      load_column(bool_v[pos_base].data());

    } else if constexpr (std::is_same_v<T, IntT>) {

      const size_t pos_base = find_col_base(matr_idx[3], 3);
      load_column(int_v[pos_base].data());

    } else if constexpr (std::is_same_v<T, UIntT>) {

      const size_t pos_base = find_col_base(matr_idx[4], 4);
      load_column(uint_v[pos_base].data());

    } else if constexpr (std::is_same_v<T, FloatT>) {

      const size_t pos_base = find_col_base(matr_idx[5], 5);
      load_column(dbl_v[pos_base].data());

    } else {
      std::cerr << "Error in (get_col), unsupported type\n";
      return;
    };

};



