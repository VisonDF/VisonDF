#pragma once

void concat(Dataframe& obj) 
{
    const unsigned int& ncol2 = obj.get_ncol();

    if (ncol != ncol2) {
      std::cerr << "Can't concatenate 2 dataframes with different number of columns\n";
      return;
    };
    
    const std::vector<char> type_refv2 = obj.get_typecol();
    
    if (type_refv != type_refv2) {
      std::cerr << "Can't concatenate 2 dataframes with different column type\n";
      return;
    };
  
    const std::vector<std::string>& str_v2 = obj.get_str_vec();
    const std::vector<char>& chr_v2 = obj.get_chr_vec();
    const std::vector<bool>& bool_v2 = obj.get_bool_vec();
    const std::vector<int>& int_v2 = obj.get_int_vec();
    const std::vector<unsigned int>& uint_v2 = obj.get_uint_vec();
    const std::vector<double>& dbl_v2 = obj.get_dbl_vec();
  
    const std::vector<std::vector<std::string>>& tmp_val_refv2 = obj.get_tmp_val_refv();
  
    const unsigned int& nrow2 = obj.get_nrow();
    unsigned int pre_nrow = nrow;
    nrow += nrow2;
  
    std::vector<std::string> new_str_v;
    new_str_v.reserve(str_v.size() + str_v2.size());
    
    for (size_t el = 0; matr_idx[0].size(); el += 1) {

      const size_t val_idx = matr_idx[0][el];

      std::vector<std::string>& val_tmp = tmp_val_refv[val_idx];
      std::vector<std::string>& val_tmp2 = tmp_val_refv2[val_idx];

      val_tmp.reserve(nrow);
      val_tmp.insert(val_tmp.end(), 
                     val_tmp2.begin(),
                     val_tmp2.end());

      const size_t base_old = el * pre_nrow;
      const size_t base_new = el * nrow2;

      new_str_v.insert(new_str_v.end(),
                     str_v.begin() + base_old,
                     str_v.begin() + base_old + pre_nrow);
      
      new_str_v.insert(new_str_v.end(),
                     str_v2.begin() + base_new,
                     str_v2.begin() + base_new + nrow2);

    };
 
    str_v.swap(new_str_v);

    std::vector<char> new_chr_v;
    new_chr_v.reserve(chr_v.size() + chr_v2.size());
  
    for (size_t el = 0; el < matr_idx[1].size(); el += 1) {
      
      const size_t val_idx = matr_idx[1][el];

      std::vector<std::string>& val_tmp = tmp_val_refv[val_idx];
      std::vector<std::string>& val_tmp2 = tmp_val_refv2[val_idx];

      val_tmp.reserve(nrow);
      val_tmp.insert(val_tmp.end(), 
                     val_tmp2.begin(),
                     val_tmp2.end());

      const size_t base_old = el * pre_nrow;
      const size_t base_new = el * nrow2;

      new_chr_v.insert(new_chr_v.end(),
                   chr_v.begin() + base_old,
                   chr_v.begin() + base_old + pre_nrow);
      
      new_chr_v.insert(new_chr_v.end(),
                     chr_v2.begin() + base_new,
                     chr_v2.begin() + base_new + nrow2);


    };
 
    chr_v.swap(new_chr_v);

    std::vector<uint8_t> new_bool_v;
    new_bool_v.reserve(bool_v.size() + bool_v2.size());
 
    for (size_t el = 0; el < matr_idx[2].size(); el += 1) {
      
      const size_t val_idx = matr_idx[2][el];

      std::vector<std::string>& val_tmp = tmp_val_refv[val_idx];
      std::vector<std::string>& val_tmp2 = tmp_val_refv2[val_idx];

      val_tmp.reserve(nrow);
      val_tmp.insert(val_tmp.end(), 
                     val_tmp2.begin(),
                     val_tmp2.end());

      const size_t base_old = el * pre_nrow;
      const size_t base_new = el * nrow2;

      new_bool_v.insert(new_bool_v.end(),
                   bool_v.begin() + base_old,
                   bool_v.begin() + base_old + pre_nrow);
      
      new_bool_v.insert(new_bool_v.end(),
                     bool_v2.begin() + base_new,
                     bool_v2.begin() + base_new + nrow2);

    };
 
    bool_v.swap(new_bool_v);
 
    std::vector<IntT> new_int_v;
    new_int_v.reserve(int_v.size() + int_v2.size());
   
    for (size_t el = 0; el < matr_idx[3].size(); el += 1) {
      
      const size_t val_idx = matr_idx[3][el];

      std::vector<std::string>& val_tmp = tmp_val_refv[val_idx];
      std::vector<std::string>& val_tmp2 = tmp_val_refv2[val_idx];

      val_tmp.reserve(nrow);
      val_tmp.insert(val_tmp.end(), 
                     val_tmp2.begin(),
                     val_tmp2.end());

      const size_t base_old = el * pre_nrow;
      const size_t base_new = el * nrow2;

      new_int_v.insert(new_int_v.end(),
                   int_v.begin() + base_old,
                   int_v.begin() + base_old + pre_nrow);
      
      new_int_v.insert(new_int_v.end(),
                     int_v2.begin() + base_new,
                     int_v2.begin() + base_new + nrow2);

    };
 
    int_v.swap(new_int_v);

    std::vector<UIntT> new_uint_v;
    new_uint_v.reserve(uint_v.size() + uint_v2.size());
   
    for (size_t el = 0; el < matr_idx[4].size(); el += 1) {
      
      const size_t val_idx = matr_idx[4][el];

      std::vector<std::string>& val_tmp = tmp_val_refv[val_idx];
      std::vector<std::string>& val_tmp2 = tmp_val_refv2[val_idx];

      val_tmp.reserve(nrow);
      val_tmp.insert(val_tmp.end(), 
                     val_tmp2.begin(),
                     val_tmp2.end());

      const size_t base_old = el * pre_nrow;
      const size_t base_new = el * nrow2;

      new_uint_v.insert(new_uint_v.end(),
                   uint_v.begin() + base_old,
                   uint_v.begin() + base_old + pre_nrow);
      
      new_uint_v.insert(new_uint_v.end(),
                     uint_v2.begin() + base_new,
                     uint_v2.begin() + base_new + nrow2);

    };
 
    uint_v.swap(new_uint_v);

    std::vector<FloatT> new_dbl_v;
    new_dbl_v.reserve(dbl_v.size() + dbl_v2.size());
   
    for (size_t el = 0; el < matr_idx[5].size(); el += 1) {
      
      const size_t val_idx = matr_idx[5][el];

      std::vector<std::string>& val_tmp = tmp_val_refv[val_idx];
      std::vector<std::string>& val_tmp2 = tmp_val_refv2[val_idx];

      val_tmp.reserve(nrow);
      val_tmp.insert(val_tmp.end(), 
                     val_tmp2.begin(),
                     val_tmp2.end());

      const size_t base_old = el * pre_nrow;
      const size_t base_new = el * nrow2;

      new_dbl_v.insert(new_dbl_v.end(),
                   dbl_v.begin() + base_old,
                   dbl_v.begin() + base_old + pre_nrow);
      
      new_dbl_v.insert(new_dbl_v.end(),
                     dbl_v2.begin() + base_new,
                     dbl_v2.begin() + base_new + nrow2);

    };
 
    dbl_v.swap(new_dbl_v);

};


