#pragma once

template <typename T, bool IsBool = false>
void get_col_filter_simd(unsigned int &x, 
                std::vector<T> &rtn_v,
                const std::vector<uint8_t> &mask) {
  
  size_t i2 = 0;
  const unsigned int n_el = mask.size();

  auto get_filtered_col = [&](auto& column_vec,
                              const std::vector<unsigned int>& col_index_vec)
  {
      using simd_t = v2::simd<T>;
      using abi_t  = typename simd_t::abi_type;
      using mask_t = v2::simd<uint8_t, abi_t>;
  
      size_t idx = 0;
      while (idx < col_index_vec.size() && x != col_index_vec[idx])
          ++idx;
  
      if (idx == col_index_vec.size()) {
          std::cerr << "Error in (get_col), no column found\n";
          return;
      }
  
      size_t base = idx * nrow;
  
      size_t active_count = 0;
      for (size_t i = 0; i < nrow; ++i)
          active_count += (mask[i] != 0);
  
      rtn_v.resize(active_count);
  
  #if defined(__ARM_FEATURE_SVE) || defined(__riscv_vector)
      const size_t W = v2::simd_size<T>();
  #else
      constexpr size_t W = v2::simd_size<T>();
  #endif
  
      size_t out_idx = 0;
      size_t i = 0;
  
  #if defined(__AVX512F__)
  
      for (; i + W < n_el; i += W) {
          mask_t m(&mask[i], v2::element_aligned);
          simd_t vals(&column_vec[base + i], v2::element_aligned);
  
          const auto active = m != 0;
          vals.copy_to(&rtn_v[out_idx], v2::element_aligned, active);
          out_idx += v2::popcount(active);
      }
  
  #elif defined(__AVX2__)
  
      for (; i + W < n_el; i += W) {
          mask_t m(&mask[i], v2::element_aligned);
          simd_t vals(&column_vec[base + i], v2::element_aligned);
  
          for (size_t k = 0; k < W; ++k)
              if (m[k])
                  rtn_v[out_idx++] = vals[k];
      }
  
  #endif
  
      for (; i < n_el; ++i)
          if (mask[i])
              rtn_v[out_idx++] = column_vec[base + i];
  };

  if constexpr (IsBool) {
    
    get_filtered_col(bool_v, matr_idx[2]);

  } else if constexpr (std::is_same_v<T, IntT>) {

    get_filtered_col(int_v, matr_idx[3]);

  } else if constexpr (std::is_same_v<T, UIntT>) {

    get_filtered_col(uint_v, matr_idx[4]);


  } else if constexpr (std::is_same_v<T, FloatT>) {

    get_filtered_col(dbl_v, matr_idx[5]);

  } else if constexpr (std::is_same_v<T, std::string>) {

    while (i2 < matr_idx[0].size()) {
      if (x == matr_idx[0][i2]) {
        break;
      };
      i2 += 1;
    };

    if (i2 == matr_idx[2].size()) {
      std::cerr << "Error in (get_col), no column found\n";
      return;
    };

    i2 = nrow * i2;

    size_t active_count = 0;
    for (size_t i = 0; i < nrow; ++i)
        active_count += mask[i] != 0;

    rtn_v.resize(active_count);

    for (size_t i = 0; i < n_el; ++i) {
      if (!mask[i]) {
        continue;
      }
      rtn_v[i] = str_v[i2 + i];
    };

  } else if constexpr (std::is_same_v<T, CharT>) {

    get_filtered_col(chr_v, matr_idx[1]);

  } else {
    std::cerr << "Error in (get_col), unsupported type\n";
    return;
  };

};


