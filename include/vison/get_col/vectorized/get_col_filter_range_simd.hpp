#pragma once

template <typename T>
void get_col_filter_range_simd(unsigned int &x, 
                std::vector<T> &rtn_v,
                const std::vector<uint8_t> &mask,
                const unsigned int& strt_vl) {
  
  size_t i2 = 0;
  const unsigned int n_el = mask.size();

  if constexpr (std::is_same_v<T, bool>) {

    while (i2 < matr_idx[2].size()) {
      if (x == matr_idx[2][i2]) {
        break;
      };
      i2 += 1;
    };

    if (i2 == matr_idx[2].size()) {
      std::cerr << "Error in (get_col), no column found\n";
      return;
    };

    i2 = nrow * i2;

    for (size_t i = 0; i < n_el; ++i) {
      if (!mask[i]) {
        continue;
      }
      rtn_v[i] = bool_v[i2 + strt_vl + i];
    };

  } else if constexpr (std::is_same_v<T, IntT>) {

    using simd_t  = v2::simd<T>;
    using abi_t   = typename simd_t::abi_type;
    using mask_t = v2::simd<uint8_t, abi_t>;
    
    while (i2 < matr_idx[3].size()) {
      if (x == matr_idx[3][i2]) {
        break;
      };
      i2 += 1;
    };

    if (i2 == matr_idx[3].size()) {
      std::cerr << "Error in (get_col), no column found\n";
      return;
    };

    i2 = nrow * i2;

    size_t active_count = 0;
    for (size_t i = 0; i < nrow; ++i)
        active_count += mask[i] != 0;

    rtn_v.resize(active_count);
    
    #if defined(__ARM_FEATURE_SVE) || defined(__riscv_vector)
        const size_t j = v2::simd_size<T>();
    #else
        constexpr size_t j = v2::simd_size<T>();
    #endif
    
    size_t out_idx = 0;
    size_t i = 0;

    #if defined(__AVX512F__)

    for (; i + j < n_el; i += j)
    {
        mask_t m(&mask[i], v2::element_aligned);
        simd_t vals(&int_v[i2 + strt_vl + i], v2::element_aligned);

        const auto active = m != 0;

        vals.copy_to(&rtn_v[out_idx], v2::element_aligned, active);
        out_idx += v2::popcount(active);
    }

    #elif defined(__AVX2__)

    for (; i + j < n_el; i += j)
    {
        mask_t m(&mask[i], v2::element_aligned);
        simd_t vals(&int_v[i2 + strt_vl + i], v2::element_aligned);

        for (size_t k = 0; k < j; ++k) {
            if (m[k]) {
                rtn_v[out_idx] = vals[k];
                out_idx += 1;
            }
        }

    }
 
    #endif

    for (; i < n_el; ++i) {
        if (mask[i]) {
          rtn_v[out_idx] = int_v[i2 + i];
          out_idx += 1;
        }
    }

  } else if constexpr (std::is_same_v<T, UIntT>) {

    using simd_t  = v2::simd<T>;
    using abi_t   = typename simd_t::abi_type;
    using mask_t = v2::simd<uint8_t, abi_t>;

    while (i2 < matr_idx[4].size()) {
      if (x == matr_idx[4][i2]) {
        break;
      };
      i2 += 1;
    };

    if (i2 == matr_idx[4].size()) {
      std::cerr << "Error in (get_col), no column found\n";
      return;
    };

    i2 = nrow * i2;

    size_t active_count = 0;
    for (size_t i = 0; i < nrow; ++i)
        active_count += mask[i] != 0;

    rtn_v.resize(active_count);
    
    #if defined(__ARM_FEATURE_SVE) || defined(__riscv_vector)
        const size_t j = v2::simd_size<T>();
    #else
        constexpr size_t j = v2::simd_size<T>();
    #endif
    
    size_t out_idx = 0;
    size_t i = 0;

    #if defined(__AVX512F__)

    for (; i + j < n_el; i += j)
    {
        mask_t m(&mask[i], v2::element_aligned);
        simd_t vals(&uint_v[i2 + strt_vl + i], v2::element_aligned);

        const auto active = m != 0;

        vals.copy_to(&rtn_v[out_idx], v2::element_aligned, active);
        out_idx += v2::popcount(active);
    }

    #elif defined(__AVX2__)

    for (; i + j < n_el; i += j)
    {
        mask_t m(&mask[i], v2::element_aligned);
        simd_t vals(&uint_v[i2 + strt_vl + i], v2::element_aligned);

        for (size_t k = 0; k < j; ++k) {
            if (m[k]) {
                rtn_v[out_idx] = vals[k];
                out_idx += 1;
            }
        }

    }
 
    #endif

    for (; i < n_el; ++i) {
        if (mask[i]) {
          rtn_v[out_idx] = uint_v[i2 + i];
          out_idx += 1;
        }
    }

  } else if constexpr (std::is_same_v<T, FloatT>) {

    using simd_t  = v2::simd<T>;
    using abi_t   = typename simd_t::abi_type;
    using mask_t = v2::simd<uint8_t, abi_t>;

    while (i2 < matr_idx[5].size()) {
      if (x == matr_idx[5][i2]) {
        break;
      };
      i2 += 1;
    };

    if (i2 == matr_idx[5].size()) {
      std::cerr << "Error in (get_col), no column found\n";
      return;
    };

    i2 = nrow * i2;

    size_t active_count = 0;
    for (size_t i = 0; i < nrow; ++i)
        active_count += mask[i] != 0;

    rtn_v.resize(active_count);
    
    #if defined(__ARM_FEATURE_SVE) || defined(__riscv_vector)
        const size_t j = v2::simd_size<T>();
    #else
        constexpr size_t j = v2::simd_size<T>();
    #endif
    
    size_t out_idx = 0;
    size_t i = 0;

    #if defined(__AVX512F__)

    for (; i + j < n_el; i += j)
    {
        mask_t m(&mask[i], v2::element_aligned);
        simd_t vals(&dbl_v[i2 + strt_vl + i], v2::element_aligned);

        const auto active = m != 0;

        vals.copy_to(&rtn_v[out_idx], v2::element_aligned, active);
        out_idx += v2::popcount(active);
    }

    #elif defined(__AVX2__)

    for (; i + j < n_el; i += j)
    {
        mask_t m(&mask[i], v2::element_aligned);
        simd_t vals(&dbl_v[i2 + strt_vl + i], v2::element_aligned);

        for (size_t k = 0; k < j; ++k) {
            if (m[k]) {
                rtn_v[out_idx] = vals[k];
                out_idx += 1;
            }
        }

    }
 
    #endif

    for (; i < n_el; ++i) {
        if (mask[i]) {
          rtn_v[out_idx] = dbl_v[i2 + i];
          out_idx += 1;
        }
    }

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

    for (size_t i = 0; i < n_el; ++i) {
      if (!mask[i]) {
        continue;
      }
      rtn_v[i] = bool_v[i];
    };

  } else if constexpr (std::is_same_v<T, char>) {

    using simd_t  = v2::simd<unsigned char>;
    using abi_t   = typename simd_t::abi_type;
    using mask_t = v2::simd<uint8_t, abi_t>;

    while (i2 < matr_idx[1].size()) {
      if (x == matr_idx[1][i2]) {
        break;
      };
      i2 += 1;
    };

    if (i2 == matr_idx[1].size()) {
      std::cerr << "Error in (get_col), no column found\n";
      return;
    };

    i2 = nrow * i2;

    size_t active_count = 0;
    for (size_t i = 0; i < nrow; ++i)
        active_count += mask[i] != 0;

    rtn_v.resize(active_count);
    
    #if defined(__ARM_FEATURE_SVE) || defined(__riscv_vector)
        const size_t j = v2::simd_size<char>();
    #else
        constexpr size_t j = v2::simd_size<char>();
    #endif
    
    size_t out_idx = 0;
    size_t i = 0;

    #if defined(__AVX512F__)

    for (; i + j < n_el; i += j)
    {
        mask_t m(&mask[i], v2::element_aligned);
        simd_t vals(&chr_v[i2 + strt_vl + i], v2::element_aligned);

        const auto active = m != 0;

        vals.copy_to(&rtn_v[out_idx], v2::element_aligned, active);
        out_idx += v2::popcount(active);
    }

    #elif defined(__AVX2__)

    for (; i + j < n_el; i += j)
    {
        mask_t m(&mask[i], v2::element_aligned);
        simd_t vals(&chr_v[i2 + strt_vl + i], v2::element_aligned);

        for (size_t k = 0; k < j; ++k) {
            if (m[k]) {
                rtn_v[out_idx] = vals[k];
                out_idx += 1;
            }
        }

    }
 
    #endif

    for (; i < n_el; ++i) {
        if (mask[i]) {
          rtn_v[out_idx] = chr_v[i2 + strt_vl + i];
          out_idx += 1;
        }
    }

  } else {
    std::cerr << "Error in (get_col), unsupported type\n";
    return;
  };

};


