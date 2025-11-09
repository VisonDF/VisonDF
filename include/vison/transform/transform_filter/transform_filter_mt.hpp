#pragma once

template<unsigned int CORES = 4>
void transform_filter(std::vector<uint8_t>& mask) 
{
  
    unsigned int nrow_local;
  
    #pragma omp parallel for num_threads(CORES) schedule(static)
    for (size_t i2 = 0 ; i2 < matr_idx[0].size(); i2 += 1) {
      const unsigned int& pos_vl = matr_idx[0][i2];
      std::vector<std::string>& val_tmp = tmp_val_refv[pos_vl];
      nrow_local = 0;
      auto* col = str_v.data() + nrow * i2;
      for (size_t i = 0; i < mask.size(); ++i) {
        if (!mask[i]) {
          continue;
        }
        val_tmp[nrow_local] = val_tmp[i];
        col[nrow_local] = col[i];
        nrow_local += 1;
      }
    };
 
   #pragma omp parallel for num_threads(CORES) schedule(static)
    for (size_t i2 = 0 ; i2 < matr_idx[1].size(); i2 += 1) {
      const unsigned int& pos_vl = matr_idx[1][i2];
      std::vector<std::string>& val_tmp = tmp_val_refv[pos_vl];
      nrow_local = 0;
      auto* col = chr_v.data() + nrow * i2;
      for (size_t i = 0; i < mask.size(); ++i) {
        if (!mask[i]) {
          continue;
        }
        val_tmp[nrow_local] = val_tmp[i];
        col[nrow_local] = col[i];
        nrow_local += 1;
      }
    };

   #pragma omp parallel for num_threads(CORES) schedule(static)
    for (size_t i2 = 0 ; i2 < matr_idx[2].size(); i2 += 1) {
      const unsigned int& pos_vl = matr_idx[2][i2];
      std::vector<std::string>& val_tmp = tmp_val_refv[pos_vl];
      nrow_local = 0;
      for (size_t i = 0; i < mask.size(); ++i) {
        if (!mask[i]) {
          continue;
        }
        val_tmp[nrow_local] = val_tmp[i];
        bool_v[nrow * i2 + nrow_local] = bool_v[nrow * i2 + i];
        nrow_local += 1;
      }
    };

    #pragma omp parallel for num_threads(CORES) schedule(static)
    for (size_t i2 = 0 ; i2 < matr_idx[3].size(); i2 += 1) {
      const unsigned int& pos_vl = matr_idx[3][i2];
      std::vector<std::string>& val_tmp = tmp_val_refv[pos_vl];
      nrow_local = 0;
      auto* col = int_v.data() + nrow * i2;
      for (size_t i = 0; i < mask.size(); ++i) {
        if (!mask[i]) {
          continue;
        }
        val_tmp[nrow_local] = val_tmp[i];
        col[nrow_local] = col[i];
        nrow_local += 1;
      }
    };

   #pragma omp parallel for num_threads(CORES) schedule(static)
    for (size_t i2 = 0 ; i2 < matr_idx[4].size(); i2 += 1) {
      const unsigned int& pos_vl = matr_idx[4][i2];
      std::vector<std::string>& val_tmp = tmp_val_refv[pos_vl];
      nrow_local = 0;
      auto* col = uint_v.data() + nrow * i2;
      for (size_t i = 0; i < mask.size(); ++i) {
        if (!mask[i]) {
          continue;
        }
        val_tmp[nrow_local] = val_tmp[i];
        col[nrow_local] = col[i];
        nrow_local += 1;
      }
    };

    #pragma omp parallel for num_threads(CORES) schedule(static)
    for (size_t i2 = 0 ; i2 < matr_idx[5].size(); i2 += 1) {
      const unsigned int& pos_vl = matr_idx[5][i2];
      std::vector<std::string>& val_tmp = tmp_val_refv[pos_vl];
      nrow_local = 0;
      auto* col = dbl_v.data() + nrow * i2;
      for (size_t i = 0; i < mask.size(); ++i) {
        if (!mask[i]) {
          continue;
        }
        val_tmp[nrow_local] = val_tmp[i];
        col[nrow_local] = col[i];
        nrow_local += 1;
      }
    };

    nrow = nrow_local;

};





