#pragma once

template <typename T>
void get_col_filter_idx_simd(unsigned int &x, 
                std::vector<T> &rtn_v,
                std::vector<unsigned int> &mask) {
  
  const unsigned int n_el = mask.size();
  rtn_v.resize(n_el);
  unsigned int i = 0;
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
    
    using simd_t = v2::simd<IntT>;
    using abi_t  = typename simd_t::abi_type;
    using mask_t = v2::simd<unsigned int, abi_t>;

    #if defined(__ARM_FEATURE_SVE) || defined(__riscv_vector)
        const size_t j = v2::simd_size<IntT>();
    #else
        constexpr size_t j = v2::simd_size<IntT>();
    #endif

    for (; i + j < n_el; i += j) {
        
      mask_t idx(&mask[i], v2::element_aligned);

      simd_t vals{};

      for (size_t k = 0; k < j; ++k)
          vals[k] = int_v[i2 + idx[k]];

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

    using simd_t = v2::simd<UIntT>;
    using abi_t  = typename simd_t::abi_type;
    using mask_t = v2::simd<unsigned int, abi_t>;
    
    #if defined(__ARM_FEATURE_SVE) || defined(__riscv_vector)
        const size_t j = v2::simd_size<UIntT>();
    #else
        constexpr size_t j = v2::simd_size<UIntT>();
    #endif

    for (; i + j < n_el; i += j) {
        
      mask_t idx(&mask[i], v2::element_aligned);

      simd_t vals{};

      for (size_t k = 0; k < j; ++k)
          vals[k] = uint_v[i2 + idx[k]];

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

    if constexpr (std::is_same_v<T, double>) {

        for (; i + 4 <= n_el; i += 4) {
            // Load 4 indices (uint32) safely without requiring alignment
            __m128i idx = _mm_loadu_si128((__m128i const*)&mask[i]); 
            // Gather 4 doubles (each 8 bytes) from dbl_v
            __m256d vals = _mm256_i32gather_pd(&dbl_v[i2], idx, 8); 
            // Store 4 doubles contiguously
            _mm256_storeu_pd(&rtn_v[i], vals);
        }

    } else if constexpr (std::is_same_v<T, float>) {

        for (; i + 8 <= n_el; i += 8) {
            // Load 8 indices for float (32-bit indices)
            __m256i idx = _mm256_loadu_si256((__m256i const*)&mask[i]);
            // Gather 8 floats (each 4 bytes) from dbl_v
            __m256 vals = _mm256_i32gather_ps(&dbl_v[i2], idx, 4);
            // Store 8 floats contiguously
            _mm256_storeu_ps(&rtn_v[i], vals);
        }

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

    for (; i + 8 <= n_el; i += 8) {
        // Load 8 indices (uint32) — unaligned safe
        __m256i idx = _mm256_loadu_si256((__m256i const*)&mask[i]);

        // Gather 8 chars (bytes) using scalar fallback into vector registers
        // There is no byte gather intrinsic in AVX2, so we must emulate it.
        alignas(32) unsigned char tmp[8];
        for (int k = 0; k < 8; ++k)
            tmp[k] = chr_v[i2 + ((uint32_t*)&idx)[k]];

        // Load 8 chars into an AVX register
        __m128i vals = _mm_loadl_epi64((__m128i*)tmp);

        // Store back to rtn_v
        _mm_storel_epi64((__m128i*)&rtn_v[i], vals);
    }

    for (; i < n_el; i += 1) {
      const size_t pos = i2 + mask[i];
      rtn_v[i] = chr_v[pos];
    }

  } else {
    std::cerr << "Error in (get_col), unsupported type\n";
    return;
  };

};


