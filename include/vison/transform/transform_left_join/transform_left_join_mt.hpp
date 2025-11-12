#pragma once

template <unsigned int CORES = 4, 
         bool SimdHash = true, 
         bool First = false>
void transform_left_join_mt(Dataframe &obj, 
                const unsigned int &key1, 
                const unsigned int &key2,
                const std::string default_str = "NA",
                const char default_chr = ' ',
                const bool default_bool = 0,
                const int default_int = 0,
                const unsigned int default_uint = 0,
                const double default_dbl = 0) 
{
  
    const unsigned int& ncol2 = obj.get_ncol();

    const std::vector<std::vector<unsigned int>>& matr_idx2 = obj.get_matr_idx();

    const std::vector<std::string>& str_v2 = obj.get_str_vec();
    const std::vector<char>& chr_v2 = obj.get_chr_vec();
    const std::vector<bool>& bool_v2 = obj.get_bool_vec();
    const std::vector<int>& int_v2 = obj.get_int_vec();
    const std::vector<unsigned int>& uint_v2 = obj.get_uint_vec();
    const std::vector<double>& dbl_v2 = obj.get_dbl_vec();
    
    const unsigned int size_str  = matr_idx[0].size();
    const unsigned int size_chr  = matr_idx[1].size();
    const unsigned int size_bool = matr_idx[2].size();
    const unsigned int size_int  = matr_idx[3].size();
    const unsigned int size_uint = matr_idx[4].size();
    const unsigned int size_dbl  = matr_idx[5].size();
    
    std::vector<std::string> vec_str(nrow, default_str);
    tmp_val_refv.insert(tmp_val_refv.end(), ncol2, vec_str);

    const std::vector<char>& vec_type = obj.get_typecol();
    const std::vector<std::string>& col1 = tmp_val_refv[key1];
    
    const std::vector<std::vector<std::string>>& tmp_val_refv2 = obj.get_tmp_val_refv();
    const std::vector<std::string>& col2 = tmp_val_refv2[key2];

    str_v.resize(str_v.size()   + nrow * size_str,  default_str);
    chr_v.resize(chr_v.size()   + nrow * size_chr,  default_chr);
    bool_v.resize(bool_v.size() + nrow * size_bool, default_bool);
    int_v.resize(int_v.size()   + nrow * size_int,  default_int);
    uint_v.resize(uint_v.size() + nrow * size_uint, default_uint);
    dbl_v.resize(dbl_v.size()   + nrow * size_dbl,  default_dbl);
 
    std::vector<std::vector<unsigned int>> matr_idx2b = matr_idx2;
    for (auto& el : matr_idx2b) {
      for (auto& el2 : el) {
        el2 += ncol;
      };
    };

    matr_idx[0].insert(matr_idx[0].end(), 
                       matr_idx2b[0].begin(), 
                       matr_idx2b[0].end());
    matr_idx[1].insert(matr_idx[1].end(), 
                       matr_idx2b[1].begin(), 
                       matr_idx2b[1].end());
    matr_idx[2].insert(matr_idx[2].end(), 
                       matr_idx2b[2].begin(), 
                       matr_idx2b[2].end());
    matr_idx[3].insert(matr_idx[3].end(), 
                       matr_idx2b[3].begin(), 
                       matr_idx2b[3].end());
    matr_idx[4].insert(matr_idx[4].end(), 
                       matr_idx2b[4].begin(), 
                       matr_idx2b[4].end());
    matr_idx[5].insert(matr_idx[5].end(), 
                       matr_idx2b[5].begin(), 
                       matr_idx2b[5].end());


    using map_t = std::conditional_t<
        SimdHash,
        ankerl::unordered_dense::map<std::string_view, size_t, simd_hash>,
        ankerl::unordered_dense::map<std::string_view, size_t>
    >;

    map_t lookup;
    lookup.reserve(col2.size());
    for (size_t i = 0; i < col2.size(); i += 1) {
      if constexpr (First) {
          lookup.try_emplace(col2[i], i);
      } else if constexpr (!First) {
          lookup[col2[i]] = i;
      }
    };

    const unsigned int& nrow2 = obj.get_nrow();

    std::vector<size_t> match_idx(nrow, SIZE_MAX);

    omp_set_dynamic(0);
    omp_set_num_threads(CORES);
    setenv("OMP_PROC_BIND", "close", 1);

    #pragma omp parallel num_threads(CORES) default(none) shared(nrow,nrow2,ncol,matr_idx2,tmp_val_refv,tmp_val_refv2, \
        str_v,str_v2,chr_v,chr_v2,bool_v,bool_v2,int_v,int_v2,uint_v,uint_v2,dbl_v,dbl_v2, \
        match_idx, lookup, col1, size_str, size_chr, size_bool, size_int, size_uint, size_dbl)
    {

        #pragma omp for schedule(static) nowait
        for (size_t i = 0; i < nrow; ++i) {
            auto it = lookup.find(col1[i]);
            if (it != lookup.end())
                match_idx[i] = it->second;
        }

        #pragma omp for schedule(static) nowait
        for (size_t t = 0; t < matr_idx2[0].size(); ++t) {
            const size_t src_col = matr_idx2[0][t];
            const size_t dst_col = ncol + src_col;

            std::vector<std::string>& val_tmp  = tmp_val_refv[dst_col];
            const std::vector<std::string>& val_tmp2 = tmp_val_refv2[src_col];

            auto* __restrict dst_val = str_v.data() + nrow * (size_str + t);
            const auto* __restrict src_val = str_v2.data() + nrow2 * t;

            for (size_t i = 0; i < nrow; ++i) {
                size_t j = match_idx[i];
                if (j != SIZE_MAX) {
                    dst_val[i] = src_val[j];
                }
            }

            for (size_t i = 0; i < nrow; ++i) {
                size_t j = match_idx[i];
                if (j != SIZE_MAX) {
                    val_tmp[i] = val_tmp2[j];
                }
            }

        }   

        #pragma omp for schedule(static) nowait
        for (size_t t = 0; t < matr_idx2[1].size(); ++t) {
            const size_t src_col = matr_idx2[1][t];
            const size_t dst_col = ncol + src_col;

            std::vector<std::string>& val_tmp  = tmp_val_refv[dst_col];
            const std::vector<std::string>& val_tmp2 = tmp_val_refv2[src_col];

            auto* __restrict dst_val = chr_v.data() + nrow * (size_chr + t);
            const auto* __restrict src_val = chr_v2.data() + nrow2 * t;

            #pragma omp simd
            for (size_t i = 0; i < nrow; ++i) {
                size_t j = match_idx[i];
                if (j != SIZE_MAX) {
                    dst_val[i] = src_val[j];
                }
            }

            for (size_t i = 0; i < nrow; ++i) {
                size_t j = match_idx[i];
                if (j != SIZE_MAX) {
                    val_tmp[i] = val_tmp2[j];
                }
            }

        }   
 
        //#pragma omp for schedule(static) nowait
        //for (size_t t = 0; t < matr_idx2[2].size(); ++t) {
        //    const size_t src_col = matr_idx2[2][t];
        //    const size_t dst_col = ncol + src_col;

        //    std::vector<std::string>& val_tmp  = tmp_val_refv[dst_col];
        //    const std::vector<std::string>& val_tmp2 = tmp_val_refv2[src_col];

        //    auto dst_val = bool_v.begin() + nrow * (size_bool + t);
        //    const auto src_val = bool_v2.begin() + nrow2 * t;

        //    for (size_t i = 0; i < nrow; ++i) {
        //        size_t j = match_idx[i];
        //        if (j != SIZE_MAX) {
        //            dst_val[i] = src_val[j];
        //        }
        //    }

        //    for (size_t i = 0; i < nrow; ++i) {
        //        size_t j = match_idx[i];
        //        if (j != SIZE_MAX) {
        //            val_tmp[i] = val_tmp2[j];
        //        }
        //    }

        //}

        #pragma omp for schedule(static) nowait
        for (size_t t = 0; t < matr_idx2[2].size(); ++t) {
            const size_t src_col = matr_idx2[2][t];
            const size_t dst_col = ncol + src_col;
        
            std::vector<std::string>& val_tmp  = tmp_val_refv[dst_col];
            const std::vector<std::string>& val_tmp2 = tmp_val_refv2[src_col];
        
            auto dst_val = bool_v.begin() + nrow * (size_bool + t);
            const auto src_val = bool_v2.begin() + nrow2 * t;
        
            #pragma omp critical(bool_write)
            {
                for (size_t i = 0; i < nrow; ++i) {
                    size_t j = match_idx[i];
                    if (j != SIZE_MAX) {
                        dst_val[i] = src_val[j];
                    }
                }
            }
        
            for (size_t i = 0; i < nrow; ++i) {
                size_t j = match_idx[i];
                if (j != SIZE_MAX) {
                    val_tmp[i] = val_tmp2[j];
                }
            }
        }
 
        #pragma omp for schedule(static) nowait
        for (size_t t = 0; t < matr_idx2[3].size(); ++t) {
            const size_t src_col = matr_idx2[3][t];
            const size_t dst_col = ncol + src_col;

            std::vector<std::string>& val_tmp  = tmp_val_refv[dst_col];
            const std::vector<std::string>& val_tmp2 = tmp_val_refv2[src_col];

            auto* __restrict dst_val = int_v.data() + nrow * (size_int + t);
            const auto* __restrict src_val = int_v2.data() + nrow2 * t;

            #pragma omp simd
            for (size_t i = 0; i < nrow; ++i) {
                size_t j = match_idx[i];
                if (j != SIZE_MAX) {
                    dst_val[i] = src_val[j];
                }
            }

            for (size_t i = 0; i < nrow; ++i) {
                size_t j = match_idx[i];
                if (j != SIZE_MAX) {
                    val_tmp[i] = val_tmp2[j];
                }
            }

        }

        #pragma omp for schedule(static) nowait
        for (size_t t = 0; t < matr_idx2[4].size(); ++t) {
            const size_t src_col = matr_idx2[4][t];
            const size_t dst_col = ncol + src_col;

            std::vector<std::string>& val_tmp  = tmp_val_refv[dst_col];
            const std::vector<std::string>& val_tmp2 = tmp_val_refv2[src_col];

            auto* __restrict dst_val = uint_v.data() + nrow * (size_uint + t);
            const auto* __restrict src_val = uint_v2.data() + nrow2 * t;

            #pragma omp simd
            for (size_t i = 0; i < nrow; ++i) {
                size_t j = match_idx[i];
                if (j != SIZE_MAX) {
                    dst_val[i] = src_val[j];
                }
            }

            for (size_t i = 0; i < nrow; ++i) {
                size_t j = match_idx[i];
                if (j != SIZE_MAX) {
                    val_tmp[i] = val_tmp2[j];
                }
            }

        }

        #pragma omp for schedule(static)
        for (size_t t = 0; t < matr_idx2[5].size(); ++t) {
            const size_t src_col = matr_idx2[5][t];
            const size_t dst_col = ncol + src_col;

            std::vector<std::string>& val_tmp  = tmp_val_refv[dst_col];
            const std::vector<std::string>& val_tmp2 = tmp_val_refv2[src_col];

            auto* __restrict dst_val = dbl_v.data() + nrow * (size_dbl + t);
            const auto* __restrict src_val = dbl_v2.data() + nrow2 * t;

            #pragma omp simd
            for (size_t i = 0; i < nrow; ++i) {
                size_t j = match_idx[i];
                if (j != SIZE_MAX) {
                    dst_val[i] = src_val[j];
                }
            }

            for (size_t i = 0; i < nrow; ++i) {
                size_t j = match_idx[i];
                if (j != SIZE_MAX) {
                    val_tmp[i] = val_tmp2[j];
                }
            }

        }

    }

    type_refv.insert(type_refv.end(), vec_type.begin(), vec_type.end());
    ncol += ncol2;
    const std::vector<std::string>& colname2 = obj.get_colname();

    if (colname2.size() > 0) {
      name_v.insert(name_v.end(), colname2.begin(), colname2.end());
    } else {
      name_v.resize(ncol);
    };

};




