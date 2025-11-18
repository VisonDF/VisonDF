#pragma once

template<unsigned int CORES = 4, bool MemClean = false>
void transform_filter_range_mt(const std::vector<uint8_t>& mask,
                               const unsigned int& strt_vl) 
{
    
    unsigned int nrow2 = nrow;

    #pragma omp parallel num_threads(CORES)
    {
      
      #pragma omp for schedule(static) nowait
      for (size_t i2 = 0 ; i2 < matr_idx[0].size(); i2 += 1) {
        const unsigned int& pos_vl = matr_idx[0][i2];
        std::vector<std::string>& val_tmp = tmp_val_refv[pos_vl];
        unsigned int nrow_local = 0;
        auto* col = str_v.data() + nrow2 * i2;
        for (size_t i = 0; i < mask.size(); ++i) {
          if (!mask[i]) {
            continue;
          }
          val_tmp[nrow_local] = std::move(val_tmp[i + strt_vl]);
          col[nrow_local] = col[i + strt_vl];
          nrow_local += 1;
        }
      };
 
      #pragma omp for schedule(static) nowait
      for (size_t i2 = 0 ; i2 < matr_idx[1].size(); i2 += 1) {
        const unsigned int& pos_vl = matr_idx[1][i2];
        std::vector<std::string>& val_tmp = tmp_val_refv[pos_vl];
        unsigned int nrow_local = 0;
        auto* col = chr_v.data() + nrow2 * i2;
        for (size_t i = 0; i < mask.size(); ++i) {
          if (!mask[i]) {
            continue;
          }
          val_tmp[nrow_local] = val_tmp[i + strt_vl];
          col[nrow_local] = col[i + strt_vl];
          nrow_local += 1;
        }
      };

      #pragma omp for schedule(static) nowait
      for (size_t i2 = 0 ; i2 < matr_idx[2].size(); i2 += 1) {
        const unsigned int& pos_vl = matr_idx[2][i2];
        std::vector<std::string>& val_tmp = tmp_val_refv[pos_vl];
        unsigned int nrow_local = 0;
        auto* col = bool_v.data() + nrow2 * i2;
        for (size_t i = 0; i < mask.size(); ++i) {
          if (!mask[i]) {
            continue;
          }
          val_tmp[nrow_local] = val_tmp[i + strt_vl];
          col[nrow_local] = col[i + strt_vl];
          nrow_local += 1;
        }
      };

      #pragma omp for schedule(static) nowait
      for (size_t i2 = 0 ; i2 < matr_idx[3].size(); i2 += 1) {
        const unsigned int& pos_vl = matr_idx[3][i2];
        std::vector<std::string>& val_tmp = tmp_val_refv[pos_vl];
        unsigned int nrow_local = 0;
        auto* col = int_v.data() + nrow2 * i2;
        for (size_t i = 0; i < mask.size(); ++i) {
          if (!mask[i]) {
            continue;
          }
          val_tmp[nrow_local] = val_tmp[i + strt_vl];
          col[nrow_local] = col[i + strt_vl];
          nrow_local += 1;
        }
      };

      #pragma omp for schedule(static) nowait
      for (size_t i2 = 0 ; i2 < matr_idx[4].size(); i2 += 1) {
        const unsigned int& pos_vl = matr_idx[4][i2];
        std::vector<std::string>& val_tmp = tmp_val_refv[pos_vl];
        unsigned int nrow_local = 0;
        auto* col = uint_v.data() + nrow2 * i2;
        for (size_t i = 0; i < mask.size(); ++i) {
          if (!mask[i]) {
            continue;
          }
          val_tmp[nrow_local] = val_tmp[i + strt_vl];
          col[nrow_local] = col[i + strt_vl];
          nrow_local += 1;
        }
      };

      #pragma omp for schedule(static) nowait
      for (size_t i2 = 0 ; i2 < matr_idx[5].size(); i2 += 1) {
        const unsigned int& pos_vl = matr_idx[5][i2];
        std::vector<std::string>& val_tmp = tmp_val_refv[pos_vl];
        unsigned int nrow_local = 0;
        auto* col = dbl_v.data() + nrow2 * i2;
        for (size_t i = 0; i < mask.size(); ++i) {
          if (!mask[i]) {
            continue;
          }
          val_tmp[nrow_local] = val_tmp[i + strt_vl];
          col[nrow_local] = col[i + strt_vl];
          nrow_local += 1;
        }
      };

    }

    if (!name_v_row.empty()) {
      unsigned int nrow_local = 0;
      for (size_t i = 0; i < mask.size(); ++i) {
        if (!mask[i]) {
          continue;
        }
        name_v_row[nrow_local] = name_v_row[i + strt_vl];
        nrow_local += 1;
      }
    }

    if constexpr (MemClean) {
      for (auto& el : tmp_val_refv) {
        el.shrink_to_fit();
      }
      
      str_v.shrink_to_fit();
      chr_v.shrink_to_fit();
      bool_v.shrink_to_fit();
      int_v.shrink_to_fit();
      uint_v.shrink_to_fit();
      dbl_v.shrink_to_fit();
      name_v_row.shrink_to_fit();

    }

    nrow = std::count(mask.begin(), mask.end(), uint8_t{1});;

};





