#pragma once

template <typename T, bool IsBool = false>
void get_col_filter_idx_simd(unsigned int &x, 
                std::vector<T> &rtn_v,
                std::vector<unsigned int> &mask) {
  
    const unsigned int n_el = mask.size();
    rtn_v.resize(n_el);
    unsigned int i = 0;
    unsigned int i2 = 0;

    auto gather_simd_general = [&](auto& col_vec,
                           const auto& col_index_vec)
    {
    
        size_t idx = 0;
        while (idx < col_index_vec.size() && x != col_index_vec[idx])
            ++idx;
    
        if (idx == col_index_vec.size()) {
            std::cerr << "Error in (get_col), no column found\n";
            return;
        }

        using simd_t = v2::simd<T>;
        using abi_t  = typename simd_t::abi_type;
        using mask_t = v2::simd<unsigned int, abi_t>;

        size_t base = idx * nrow;
    
    #if defined(__ARM_FEATURE_SVE) || defined(__riscv_vector)
        const size_t W = v2::simd_size<T>();
    #else
        constexpr size_t W = v2::simd_size<T>();
    #endif
    
        for (; i + W < n_el; i += W)
        {
            mask_t idxv(&mask[i], v2::element_aligned);
    
            simd_t vals{};
            for (size_t k = 0; k < W; ++k)
                vals[k] = col_vec[base + idxv[k]];
    
            vals.copy_to(&rtn_v[i], v2::element_aligned);
        }
    
        for (; i < n_el; ++i)
            rtn_v[i] = col_vec[base + mask[i]];
    };

    auto gather_simd_u8 = [&](auto& col_vec, const auto& col_index_vec)
    {
        // --- 1. Locate column ---
        size_t idx = 0;
        while (idx < col_index_vec.size() && x != col_index_vec[idx])
            ++idx;
    
        if (idx == col_index_vec.size()) {
            std::cerr << "Error in (get_col), no column found\n";
            return;
        }
    
        size_t base = idx * nrow;
    
        size_t i = 0;
    
    #if defined(__AVX2__)
        // --- 2. AVX2 block: process 8 bytes at a time ---
        for (; i + 8 <= n_el; i += 8)
        {
            // load 8 indices
            __m256i idxv = _mm256_loadu_si256((__m256i const*)&mask[i]);
    
            // emulate byte gather: AVX2 cannot gather 8-bit elements
            alignas(32) unsigned char tmp[8];
            const uint32_t* idx_ptr = reinterpret_cast<const uint32_t*>(&idxv);
    
            for (int k = 0; k < 8; ++k)
                tmp[k] = col_vec[base + idx_ptr[k]];
    
            // load result back into 128-bit register
            __m128i vals = _mm_loadl_epi64((__m128i*)tmp);
    
            // store 8 bytes to output
            _mm_storel_epi64((__m128i*)&rtn_v[i], vals);
        }
    #endif
    
        // --- 3. Scalar tail ---
        for (; i < n_el; ++i)
            rtn_v[i] = col_vec[base + mask[i]];
    };

    if constexpr (IsBool) {
      
        gather_simd_u8(bool_v, matr_idx[2]);

    } else if constexpr (std::is_same_v<T, IntT>) {

        gather_simd_general(int_v, matr_idx[3]);

    } else if constexpr (std::is_same_v<T, UIntT>) {

        gather_simd_general(uint_v, matr_idx[4]);

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

    } else if constexpr (std::is_same_v<T, CharT>) {
 
        gather_simd_general(chr_v, matr_idx[1]);

    } else {
      std::cerr << "Error in (get_col), unsupported type\n";
      return;
    };

};


