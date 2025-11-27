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
    const std::vector<CharT>& chr_v2 = obj.get_chr_vec();
    const std::vector<uint8_t>& bool_v2 = obj.get_bool_vec();
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

    std::vector<CharT> new_chr_v;
    new_chr_v.resize(chr_v.size() + chr_v2.size());
  
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

        CharT* dst = new_chr_v.data() + el * nrow;
        std::memcpy(
            dst,
            chr_v.data() + base_old,
            pre_nrow * sizeof(CharT)
        );

        CharT* dst2 = new_chr_v.data() + el * nrow + pre_nrow;
        std::memcpy(
            dst2,
            chr_v2.data() + base_new,
            nrow2 * sizeof(CharT)
        );

    };
 
    chr_v.swap(new_chr_v);

    std::vector<uint8_t> new_bool_v;
    new_bool_v.resize(bool_v.size() + bool_v2.size());
 
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

        uint8_t* dst = new_bool_v.data() + el * nrow;
        std::memcpy(
            dst,
            bool_v.data() + base_old,
            pre_nrow * sizeof(uint8_t)
        );

        uint8_t* dst2 = new_bool_v.data() + el * nrow + pre_nrow;
        std::memcpy(
            dst2,
            bool_v2.data() + base_new,
            nrow2 * sizeof(uint8_t)
        );

    };
 
    bool_v.swap(new_bool_v);
 
    std::vector<IntT> new_int_v;
    new_int_v.resize(int_v.size() + int_v2.size());
   
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

        IntT* dst = new_int_v.data() + el * nrow;
        std::memcpy(
            dst,
            int_v.data() + base_old,
            pre_nrow * sizeof(IntT)
        );

        IntT* dst2 = new_int_v.data() + el * nrow + pre_nrow;
        std::memcpy(
            dst2,
            int_v2.data() + base_new,
            nrow2 * sizeof(IntT)
        );

    };
 
    int_v.swap(new_int_v);

    std::vector<UIntT> new_uint_v;
    new_uint_v.resize(uint_v.size() + uint_v2.size());
   
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

        UIntT* dst = new_uint_v.data() + el * nrow;
        std::memcpy(
            dst,
            uint_v.data() + base_old,
            pre_nrow * sizeof(UIntT)
        );

        UIntT* dst2 = new_uint_v.data() + el * nrow + pre_nrow;
        std::memcpy(
            dst2,
            uint_v2.data() + base_new,
            nrow2 * sizeof(UIntT)
        );

    };
 
    uint_v.swap(new_uint_v);

    std::vector<FloatT> new_dbl_v;
    new_dbl_v.resize(dbl_v.size() + dbl_v2.size());
   
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

        FloatT* dst = new_dbl_v.data() + el * nrow;
        std::memcpy(
            dst,
            dbl_v.data() + base_old,
            pre_nrow * sizeof(FloatT)
        );

        FloatT* dst2 = new_dbl_v.data() + el * nrow + pre_nrow;
        std::memcpy(
            dst2,
            dbl_v2.data() + base_new,
            nrow2 * sizeof(FloatT)
        );

    };
 
    dbl_v.swap(new_dbl_v);

};


