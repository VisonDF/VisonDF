#pragma once

template <bool MemClean = false>
void transform_filter_idx(std::vector<unsigned int>& mask) 
{
  
    unsigned int nrow_local;
  
    for (size_t i2 = 0 ; i2 < matr_idx[0].size(); i2 += 1) {
      const unsigned int& pos_vl = matr_idx[0][i2];
      std::vector<std::string>& val_tmp = tmp_val_refv[pos_vl];
      const std::vector<std::string> val_tmp2 = val_tmp;
      nrow_local = 0;
      auto* col = str_v.data() + nrow * i2;
      const auto col2 = std::vector<std::string>(str_v.data() + nrow * i2, str_v.data() + nrow * (i2 + 1));
      for (auto& pos_idx : mask) {
        val_tmp[nrow_local] = val_tmp2[pos_idx];
        col[nrow_local] = col2[pos_idx];
        nrow_local += 1;
      }
    };
  
    for (size_t i2 = 0 ; i2 < matr_idx[1].size(); i2 += 1) {
      const unsigned int& pos_vl = matr_idx[1][i2];
      std::vector<std::string>& val_tmp = tmp_val_refv[pos_vl];
      const std::vector<std::string> val_tmp2 = val_tmp;
      nrow_local = 0;
      auto* col = chr_v.data() + nrow * i2;
      const auto col2 = std::vector<char>(chr_v.data() + nrow * i2, chr_v.data() + nrow * (i2 + 1));
      for (auto& pos_idx : mask) {
        val_tmp[nrow_local] = val_tmp2[pos_idx];
        col[nrow_local] = col2[pos_idx];
        nrow_local += 1;
      }
    };

    for (size_t i2 = 0 ; i2 < matr_idx[2].size(); i2 += 1) {
      const unsigned int& pos_vl = matr_idx[2][i2];
      std::vector<std::string>& val_tmp = tmp_val_refv[pos_vl];
      const std::vector<std::string> val_tmp2 = val_tmp;
      const auto col2 = std::vector<bool>(bool_v.begin() + nrow * i2, bool_v.begin() + nrow * (i2 + 1));
      nrow_local = 0;
      for (auto& pos_idx : mask) {
        val_tmp[nrow_local] = val_tmp2[pos_idx];
        bool_v[nrow * i2 + nrow_local] = col2[pos_idx];
        nrow_local += 1;
      }
    };

    for (size_t i2 = 0 ; i2 < matr_idx[3].size(); i2 += 1) {
      const unsigned int& pos_vl = matr_idx[3][i2];
      std::vector<std::string>& val_tmp = tmp_val_refv[pos_vl];
      const std::vector<std::string> val_tmp2 = val_tmp;
      nrow_local = 0;
      auto* col = int_v.data() + nrow * i2;
      const auto col2 = std::vector<IntT>(int_v.data() + nrow * i2, int_v.data() + nrow * (i2 + 1));
      for (auto& pos_idx : mask) {
        val_tmp[nrow_local] = val_tmp2[pos_idx];
        col[nrow_local] = col2[pos_idx];
        nrow_local += 1;
      }
    };

    for (size_t i2 = 0 ; i2 < matr_idx[4].size(); i2 += 1) {
      const unsigned int& pos_vl = matr_idx[4][i2];
      std::vector<std::string>& val_tmp = tmp_val_refv[pos_vl];
      const std::vector<std::string> val_tmp2 = val_tmp;
      nrow_local = 0;
      auto* col = uint_v.data() + nrow * i2;
      const auto col2 = std::vector<UIntT>(uint_v.data() + nrow * i2, uint_v.data() + nrow * (i2 + 1));
      for (auto& pos_idx : mask) {
        val_tmp[nrow_local] = val_tmp2[pos_idx];
        col[nrow_local] = col2[pos_idx];
        nrow_local += 1;
      }
    };


    for (size_t i2 = 0 ; i2 < matr_idx[5].size(); i2 += 1) {
      const unsigned int& pos_vl = matr_idx[5][i2];
      std::vector<std::string>& val_tmp = tmp_val_refv[pos_vl];
      const std::vector<std::string> val_tmp2 = val_tmp;
      nrow_local = 0;
      auto* col = dbl_v.data() + nrow * i2;
      const auto col2 = std::vector<FloatT>(dbl_v.data() + nrow * i2, dbl_v.data() + nrow * (i2 + 1));
      for (auto& pos_idx : mask) {
        val_tmp[nrow_local] = val_tmp2[pos_idx];
        col[nrow_local] = col2[pos_idx];
        nrow_local += 1;
      }
    };

    if (!name_v_row.empty()) {
      nrow_local = 0;
      std::vector<std::string> name_v_row2 = name_v_row;
      for (auto& pos_idx : mask) {
        name_v_row[nrow_local] = name_v_row2[pos_idx];
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

    nrow = mask.size();

};





