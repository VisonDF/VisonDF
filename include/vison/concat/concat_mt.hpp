#pragma once

template <unsigned int CORES = 4>
void concat_mt(Dataframe& obj) 
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
    const std::vector<IntT>& int_v2 = obj.get_int_vec();
    const std::vector<UIntT>& uint_v2 = obj.get_uint_vec();
    const std::vector<FloatT>& dbl_v2 = obj.get_dbl_vec();
  
    const std::vector<std::vector<std::string>>& tmp_val_refv2 = obj.get_tmp_val_refv();
  
    const unsigned int& nrow2 = obj.get_nrow();
    unsigned int pre_nrow = nrow;
    nrow += nrow2;
  
    std::vector<std::string> new_str_v;
    new_str_v.resize(str_v.size() + str_v2.size());

    #pragma omp parallel for num_threads(CORES)
    for (size_t el = 0; el < matr_idx[0].size(); el += 1) {

        const size_t val_idx = matr_idx[0][el];

        std::vector<std::string>& val_tmp = tmp_val_refv[val_idx];
        std::vector<std::string>& val_tmp2 = tmp_val_refv2[val_idx];

        val_tmp.reserve(nrow);
        val_tmp.insert(val_tmp.end(), 
                       val_tmp2.begin(),
                       val_tmp2.end());

        const size_t base_old = el * pre_nrow;
        const size_t base_new = el * nrow2;

        size_t i2 = base_old;
        const size_t target_1 = el * nrow + pre_nrow;
        size_t i = el * nrow;
        for (; i < target_1; ++i, ++i2)
            new_str_v[i] = str_v[i2];

        i2 = base_new;
        const size_t target_2 = el * nrow + nrow;
        for (; i < target_2; ++i, ++i2)
            new_str_v[i] = str_v2[i2];

    };
 
    str_v.swap(new_str_v);

    auto concat_pod_column = [&](auto& out_vec,
                                 const auto& vec1,
                                 const auto& vec2,
                                 const std::vector<size_t>& col_idx) 
    {
        using T = typename std::remove_reference_t<decltype(vec1)>::value_type;
    
        out_vec.resize(vec1.size() + vec2.size());
   
        #pragma omp parallel for num_threads(CORES)
        for (size_t el = 0; el < col_idx.size(); ++el) 
        {
            const size_t val_idx = col_idx[el];
    
            auto& val_tmp  = tmp_val_refv[val_idx];
            auto& val_tmp2 = tmp_val_refv2[val_idx];
    
            val_tmp.reserve(nrow);
            val_tmp.insert(val_tmp.end(),
                           val_tmp2.begin(),
                           val_tmp2.end());
    
            const size_t base_old = el * pre_nrow;
            const size_t base_new = el * nrow2;
            T* dst  = out_vec.data() + el * nrow;
            T* dst2 = dst + pre_nrow;
    
            std::memcpy(dst,
                        vec1.data() + base_old,
                        pre_nrow * sizeof(T));
    
            std::memcpy(dst2,
                        vec2.data() + base_new,
                        nrow2 * sizeof(T));
        }
    };

    std::vector<CharT> new_chr_v;
    concat_pod_column(new_chr_v, chr_v, chr_v2, matr_idx[1]);
    chr_v.swap(new_chr_v);

    std::vector<uint8_t> new_bool_v;
    concat_pod_column(new_chr_v, chr_v, chr_v2, matr_idx[1]);
    bool_v.swap(new_bool_v);
 
    std::vector<IntT> new_int_v;
    concat_pod_column(new_int_v, int_v, int_v2, matr_idx[3]);
    int_v.swap(new_int_v);

    std::vector<UIntT> new_uint_v;
    concat_pod_column(new_uint_v, uint_v, uint_v2, matr_idx[4]);
    uint_v.swap(new_uint_v);

    std::vector<FloatT> new_dbl_v;
    concat_pod_column(new_dbl_v, dbl_v, dbl_v2, matr_idx[5]);
    dbl_v.swap(new_dbl_v);

};


