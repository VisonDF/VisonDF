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
  
    const std::vector<std::vector<std::string>>& str_v2  = obj.get_str_vec() ;
    const std::vector<std::vector<CharT>>&       chr_v2  = obj.get_chr_vec() ;
    const std::vector<std::vector<uint8_t>>&     bool_v2 = obj.get_bool_vec();
    const std::vector<std::vector<IntT>>&        int_v2  = obj.get_int_vec() ;
    const std::vector<std::vector<UIntT>>&       uint_v2 = obj.get_uint_vec();
    const std::vector<std::vector<FloatT>>&      dbl_v2  = obj.get_dbl_vec() ;
  
    const std::vector<std::vector<std::string>>& tmp_val_refv2 = obj.get_tmp_val_refv();
  
    const unsigned int& nrow2 = obj.get_nrow();
    unsigned int pre_nrow = nrow;
    nrow += nrow2;

    for (auto& el : str_v ) { el.resize(nrow) };
    for (auto& el : chr_v ) { el.resize(nrow) };
    for (auto& el : bool_v) { el.resize(nrow) };
    for (auto& el : int_v ) { el.resize(nrow) };
    for (auto& el : uint_v) { el.resize(nrow) };
    for (auto& el : dbl_v ) { el.resize(nrow) };

    for (size_t el = 0; el < matr_idx[0].size(); el += 1) {

        const size_t val_idx = matr_idx[0][el];

        std::vector<std::string>& val_tmp = tmp_val_refv[val_idx];
        std::vector<std::string>& val_tmp2 = tmp_val_refv2[val_idx];

        val_tmp.reserve(nrow);
        val_tmp.insert(val_tmp.end(), 
                       val_tmp2.begin(),
                       val_tmp2.end());

        auto& dst = str_v [el];
        auto& src = str_v2[el];

        size_t i2 = 0;
        for (size_t i = pre_nrow; i < nrow; ++i, ++i2)
            dst[i] = src[i2];

    };
 
    auto concat_pod_column = [&](const auto& vec1,
                                 const auto& vec2,
                                 const std::vector<size_t>& col_idx) 
    {
        using T = typename std::remove_reference_t<decltype(vec1)>::value_type;
    
        for (size_t el = 0; el < col_idx.size(); ++el) 
        {
            const size_t val_idx = col_idx[el];
    
            auto& val_tmp  = tmp_val_refv[val_idx];
            auto& val_tmp2 = tmp_val_refv2[val_idx];
    
            val_tmp.reserve(nrow);
            val_tmp.insert(val_tmp.end(),
                           val_tmp2.begin(),
                           val_tmp2.end());
   
            T* dst = vec1[el].data() + pre_nrow;
            T* src = vec2[el].data();
            std::memcpy(dst,
                        src,
                        nrow2 * sizeof(T));
        }

    };

    concat_pod_column(chr_v , chr_v2 ,  matr_idx[1]);
    concat_pod_column(bool_v, bool_v2,  matr_idx[2]);
    concat_pod_column(int_v , int_v2 ,  matr_idx[3]);
    concat_pod_column(uint_v, uint_v2,  matr_idx[4]);
    concat_pod_column(dbl_v , dbl_v2 ,  matr_idx[5]);

};




