#pragma once

template <bool MemClean = false>
void transform_filter(std::vector<uint8_t>& mask) 
{
  
    unsigned int nrow_local;
  
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

    if (!name_v_row.empty()) {
      nrow_local = 0;
      for (size_t i = 0; i < mask.size(); ++i) {
        if (!mask[i]) {
          continue;
        }
        name_v_row[nrow_local] = name_v_row[i];
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

    nrow = nrow_local;

};





