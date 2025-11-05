#pragma once

template <typename T>
void get_col_filter_idx(unsigned int &x, 
                std::vector<T> &rtn_v,
                std::vector<unsigned int> &mask) {
  
  const unsigned int n_el = mask.size();
  rtn_v.resize(n_el);
  unsigned int i;
  unsigned int i2 = 0;

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

    for (i = 0; i < n_el; ++i) {
      const size_t pos_idx = i2 + mask[i];
      rtn_v[i] = bool_v[pos_idx];
    };

  } else if constexpr (std::is_same_v<T, IntT>) {

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
    
    #if defined(__ARM_FEATURE_SVE) || defined(__riscv_vector)
        const size_t j = v2::simd_size<unsigned int>();
    #else
        constexpr size_t j = v2::simd_size<unsigned int>();
    #endif

    for (; i + j < n_el; i += j) {
        
        v2::simd<unsigned int> idx(&mask[i], v2::element_aligned);
    
        v2::simd<IntT> vals;
    
        for (size_t k = 0; k < j; ++k) {
            const size_t pos = i2 + idx[k];
            vals[k] = int_v[pos];
        }
    
        vals.copy_to(&rtn_v[i], v2::element_aligned);
    }

    for (; i < n_el; i += 1) {
      const size_t pos = i2 + mask[i];
      rtn_v[i] = int_v[pos];
    }

  } else if constexpr (std::is_same_v<T, UIntT>) {

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

    #if defined(__ARM_FEATURE_SVE) || defined(__riscv_vector)
        const size_t j = v2::simd_size<unsigned int>();
    #else
        constexpr size_t j = v2::simd_size<unsigned int>();
    #endif

    for (; i + j < n_el; i += j) {
        
        v2::simd<unsigned int> idx(&mask[i], v2::element_aligned);
    
        v2::simd<UIntT> vals;
    
        for (size_t k = 0; k < j; ++k) {
            const size_t pos = i2 + idx[k];
            vals[k] = uint_v[pos];
        }
    
        vals.copy_to(&rtn_v[i], v2::element_aligned);
    }

    for (; i < n_el; i += 1) {
      const size_t pos = i2 + mask[i];
      rtn_v[i] = uint_v[pos];
    }

  } else if constexpr (std::is_same_v<T, FloatT>) {

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

    #if defined(__ARM_FEATURE_SVE) || defined(__riscv_vector)
        const size_t j = v2::simd_size<unsigned int>();
    #else
        constexpr size_t j = v2::simd_size<unsigned int>();
    #endif

    for (; i + j < n_el; i += j) {
        
        v2::simd<unsigned int> idx(&mask[i], v2::element_aligned);
    
        v2::simd<FloatT> vals;
    
        for (size_t k = 0; k < j; ++k) {
            const size_t pos = i2 + idx[k];
            vals[k] = dbl_v[pos];
        }
    
        vals.copy_to(&rtn_v[i], v2::element_aligned);
    }

    for (; i < n_el; i += 1) {
      const size_t pos = i2 + mask[i];
      rtn_v[i] = dbl_v[pos];
    }

  } else if constexpr (std::is_same_v<T, std::string>) {

    while (i2 < matr_idx[0].size()) {
      if (x == matr_idx[0][i2]) {
        break;
      };
      i2 += 1;
    };

    if (i2 == matr_idx[0].size()) {
      std::cerr << "Error in (get_col), no column found\n";
      return;
    };

    i2 = nrow * i2;

    for (i = 0; i < n_el; ++i) {
      const size_t pos_idx = i2 + mask[i];
      rtn_v[i] = str_v[pos_idx];
    };

  } else if constexpr (std::is_same_v<T, char>) {

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

    #if defined(__ARM_FEATURE_SVE) || defined(__riscv_vector)
        const size_t j = v2::simd_size<unsigned int>();
    #else
        constexpr size_t j = v2::simd_size<unsigned int>();
    #endif

    for (; i + j < n_el; i += j) {
        
        v2::simd<unsigned int> idx(&mask[i], v2::element_aligned);
    
        v2::simd<FloatT> vals;
    
        for (size_t k = 0; k < j; ++k) {
            const size_t pos = i2 + idx[k];
            vals[k] = dbl_v[pos];
        }
    
        vals.copy_to(&rtn_v[i], v2::element_aligned);
    }

    for (; i < n_el; i += 1) {
      const size_t pos = i2 + mask[i];
      rtn_v[i] = dbl_v[pos];
    }

  } else {
    std::cerr << "Error in (get_col), unsupported type\n";
    return;
  };

};


