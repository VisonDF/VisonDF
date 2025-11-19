#pragma once

template <unsigned int CORES = 4, bool Nested = true, bool SimdHash = true>
void otm_mt(Dataframe &obj_l,
            Dataframe &obj_r,
            const unsigned int &key1, 
            const unsigned int &key2,
            const std::string default_str = "NA",
            const char default_chr = ' ',
            const bool default_bool = 0,
            const int default_int = 0,
            const unsigned int default_uint = 0,
            const double default_dbl = 0) 
{
  
    const unsigned int& ncol1 = obj_l.get_ncol();
    const unsigned int& ncol2 = obj_r.get_ncol();

    const unsigned int& nrow1 = obj_l.get_nrow();
    const unsigned int& nrow2 = obj_r.get_nrow();

    const std::vector<std::vector<unsigned int>>& matr_idx1 = obj_l.get_matr_idx();
    const std::vector<std::vector<unsigned int>>& matr_idx2 = obj_r.get_matr_idx();

    const std::vector<std::string>& str_v2   = obj_r.get_str_vec();
    const std::vector<char>& chr_v2          = obj_r.get_chr_vec();
    const std::vector<uint8_t>& bool_v2         = obj_r.get_bool_vec();
    const std::vector<IntT>& int_v2           = obj_r.get_int_vec();
    const std::vector<UIntT>& uint_v2 = obj_r.get_uint_vec();
    const std::vector<FloatT>& dbl_v2        = obj_r.get_dbl_vec();
 
    const std::vector<std::string>& str_v1   = obj_l.get_str_vec();
    const std::vector<char>& chr_v1          = obj_l.get_chr_vec();
    const std::vector<uint8_t>& bool_v1         = obj_l.get_bool_vec();
    const std::vector<IntT>& int_v1           = obj_l.get_int_vec();
    const std::vector<UIntT>& uint_v1 = obj_l.get_uint_vec();
    const std::vector<FloatT>& dbl_v1        = obj_l.get_dbl_vec();
   
    const unsigned int size_str1  = matr_idx1[0].size();
    const unsigned int size_chr1  = matr_idx1[1].size();
    const unsigned int size_bool1 = matr_idx1[2].size();
    const unsigned int size_int1  = matr_idx1[3].size();
    const unsigned int size_uint1 = matr_idx1[4].size();
    const unsigned int size_dbl1  = matr_idx1[5].size();

    const unsigned int size_str2  = matr_idx2[0].size();
    const unsigned int size_chr2  = matr_idx2[1].size();
    const unsigned int size_bool2 = matr_idx2[2].size();
    const unsigned int size_int2  = matr_idx2[3].size();
    const unsigned int size_uint2 = matr_idx2[4].size();
    const unsigned int size_dbl2  = matr_idx2[5].size();

    const std::vector<char>& vec_type1 = obj_l.get_typecol();
    const std::vector<char>& vec_type2 = obj_r.get_typecol();

    const std::vector<std::vector<std::string>>& tmp_val_refv1 = obj_l.get_tmp_val_refv();
    const std::vector<std::vector<std::string>>& tmp_val_refv2 = obj_r.get_tmp_val_refv();

    const std::vector<std::string>& col1 = tmp_val_refv1[key1]; 
    const std::vector<std::string>& col2 = tmp_val_refv2[key2];

    std::vector<std::vector<unsigned int>> matr_idx2b = matr_idx2;
    for (auto& el : matr_idx2b) {
      for (auto& el2 : el) {
        el2 += ncol1;
      };
    };

    matr_idx[0].insert(matr_idx[0].end(), 
                       matr_idx1[0].begin(), 
                       matr_idx1[0].end());
    matr_idx[1].insert(matr_idx[1].end(), 
                       matr_idx1[1].begin(), 
                       matr_idx1[1].end());
    matr_idx[2].insert(matr_idx[2].end(), 
                       matr_idx1[2].begin(), 
                       matr_idx1[2].end());
    matr_idx[3].insert(matr_idx[3].end(), 
                       matr_idx1[3].begin(), 
                       matr_idx1[3].end());
    matr_idx[4].insert(matr_idx[4].end(), 
                       matr_idx1[4].begin(), 
                       matr_idx1[4].end());
    matr_idx[5].insert(matr_idx[5].end(), 
                       matr_idx1[5].begin(), 
                       matr_idx1[5].end());

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
        ankerl::unordered_dense::map<std::string_view, std::vector<size_t>, simd_hash>,
        ankerl::unordered_dense::map<std::string_view, std::vector<size_t>>
    >;

    map_t lookup;
    lookup.reserve(col2.size());
    for (size_t i = 0; i < col2.size(); i += 1) {
      auto [it, inserted] = lookup.try_emplace(col2[i]);
      if (inserted) {
        it->second.reserve(2);
      }
      it->second.push_back(i);
    };

    nrow = 0;

    const int prev_nested = omp_get_nested();
    const int prev_max_levels = omp_get_max_active_levels();

    if constexpr (Nested)
        omp_set_nested(1);
    else
        omp_set_nested(0);

    unsigned int outer_threads = std::min<unsigned int>(
        matr_idx1[0].size() + matr_idx2b[0].size(),
        std::max(1u, CORES / 2)
    );
    unsigned int inner_threads = std::max(1u, CORES / outer_threads);

    omp_set_max_active_levels(Nested ? 2 : 1);

    std::vector<std::vector<size_t>> match_idx(nrow1);
    std::vector<size_t> rep_v(nrow1);
    
    #pragma omp parallel for num_threads(outer_threads) schedule(static)
    for (ptrdiff_t i = 0; i < static_cast<ptrdiff_t>(nrow1); ++i) {
        auto it = lookup.find(col1[i]);
        if (it != lookup.end()) {
            match_idx[i] = it->second;          
            rep_v[i]     = match_idx[i].size(); 
        } else {
            rep_v[i] = 1;
        }
    }

    // calculate the tot number of rows safely
    std::vector<size_t> out_offset(nrow1, 0);
    for (size_t i = 1; i < nrow1; ++i) {
       out_offset[i] = out_offset[i-1] + rep_v[i-1];
    }
    size_t nrow = out_offset.back() + rep_v.back();

    #pragma omp parallel for num_threads(outer_threads) schedule(static)
    for (ptrdiff_t i = 0; i < static_cast<ptrdiff_t>(nrow1); ++i) {
        auto& v = match_idx[i];
        if (v.size() > 4)
            std::sort(v.begin(), v.end());
    }

    str_v. resize(nrow * (size_str1  + size_str2),  default_str);
    chr_v. resize(nrow * (size_chr1  + size_chr2),  default_chr);
    bool_v.resize(nrow * (size_bool1 + size_bool2),  default_bool);
    int_v. resize(nrow * (size_int1  + size_int2),  default_int);
    uint_v.resize(nrow * (size_uint1 + size_uint2),  default_uint);
    dbl_v. resize(nrow * (size_dbl1  + size_dbl2),  default_dbl);

    std::vector<std::string> vec_str;
    vec_str.resize(nrow, default_str);
    tmp_val_refv.insert(tmp_val_refv.end(), ncol1 + ncol2, vec_str);

    auto expand_repeats = [&](auto& dst_vec,
                              auto& src_vec,
                              const std::vector<size_t>& idx_list,
                              auto& tmp_val_refv,
                              auto& tmp_val_refv1)
    {
        using T = typename std::remove_reference_t<decltype(dst_vec)>::value_type;
    
        #pragma omp parallel for num_threads(outer_threads) schedule(static)
        for (ptrdiff_t t = 0; t < static_cast<ptrdiff_t>(idx_list.size()); ++t) {
    
            const size_t dst_col = idx_list[t];
    
            auto&       val_tmp  = tmp_val_refv[dst_col];
            const auto& val_tmp2 = tmp_val_refv1[dst_col];
    
            T*       dst_val = dst_vec.data()  + nrow  * static_cast<size_t>(t);
            const T* src_val = src_vec.data() + nrow1 * static_cast<size_t>(t);
    
            if constexpr (Nested) {
    
                #pragma omp parallel for num_threads(inner_threads) schedule(static)
                for (size_t i_ref = 0; i_ref < nrow1; ++i_ref) {
                    const size_t repeat = rep_v[i_ref];
    
                    const T& v1              = src_val[i_ref];
                    const std::string& v2    = val_tmp2[i_ref];
                    const size_t out         = out_offset[i_ref];
    
                    #pragma omp simd
                    for (size_t r = 0; r < repeat; ++r)
                        dst_val[out + r] = v1;
    
                    for (size_t r = 0; r < repeat; ++r)
                        val_tmp[out + r] = v2;
                }
    
            } else {
    
                size_t out = 0;
    
                for (size_t i_ref = 0; i_ref < nrow1; ++i_ref) {
                    const size_t repeat     = rep_v[i_ref];
    
                    const T& v1             = src_val[i_ref];
                    const std::string& v2   = val_tmp2[i_ref];
    
                    size_t pre_out = out;
    
                    #pragma omp simd
                    for (size_t r = 0; r < repeat; ++r)
                        dst_val[pre_out + r] = v1;
    
                    out += repeat;
                    pre_out = out - repeat;
    
                    for (size_t r = 0; r < repeat; ++r)
                        val_tmp[pre_out + r] = v2;
                }
            }
        }
    };

    auto expand_matches = [&](auto& dst_vec,
                              auto& src_vec,
                              const std::vector<size_t>& idx_list_b,
                              const std::vector<size_t>& idx_list,
                              auto& tmp_val_refv,
                              auto& tmp_val_refv2,
                              size_t offset)
    {
        using T = typename std::remove_reference_t<decltype(dst_vec)>::value_type;
    
        #pragma omp parallel for num_threads(outer_threads) schedule(static)
        for (ptrdiff_t t = 0; t < static_cast<ptrdiff_t>(idx_list_b.size()); ++t) {
    
            const size_t dst_col = idx_list_b[t];
            const size_t src_col = idx_list[t];
    
            auto&       val_tmp  = tmp_val_refv[dst_col];
            const auto& val_tmp2 = tmp_val_refv2[src_col];
    
            T*       dst_val = dst_vec.data()  + nrow  * (offset + static_cast<size_t>(t));
            const T* src_val = src_vec.data() + nrow2 * static_cast<size_t>(t);
    
            if constexpr (Nested) {
    
                #pragma omp parallel for num_threads(inner_threads) schedule(static)
                for (ptrdiff_t i_ref = 0; i_ref < static_cast<ptrdiff_t>(nrow1); ++i_ref) {
    
                    size_t out        = out_offset[static_cast<size_t>(i_ref)];
                    auto& matches     = match_idx[static_cast<size_t>(i_ref)];
    
                    if (matches.empty())
                        continue;
    
                    if (matches.size() <= 4) {
                        for (size_t j_idx : matches) {
                            dst_val[out] = src_val[j_idx];
                            val_tmp[out] = val_tmp2[j_idx];
                            ++out;
                        }
                        continue;
                    }
    
                    // Large case: contiguous-run compression
                    for (size_t k = 0; k < matches.size();) {
                        size_t start   = matches[k];
                        size_t run_len = 1;
    
                        while (k + run_len < matches.size() &&
                               matches[k + run_len] == matches[k + run_len - 1] + 1)
                            ++run_len;
    
                        std::memcpy(dst_val + out,
                                    src_val + start,
                                    run_len * sizeof(T));
    
                        std::copy_n(val_tmp2.begin() + start,
                                    run_len,
                                    val_tmp.begin() + out);
    
                        out += run_len;
                        k   += run_len;
                    }
                }
    
            } else {
    
                size_t out = 0;
    
                for (size_t i_ref = 0; i_ref < nrow1; ++i_ref) {
    
                    auto& matches = match_idx[i_ref];
    
                    if (matches.empty()) {
                        ++out;
                        continue;
                    }
    
                    if (matches.size() <= 4) {
                        for (size_t j_idx : matches) {
                            dst_val[out] = src_val[j_idx];
                            val_tmp[out] = val_tmp2[j_idx];
                            ++out;
                        }
                        continue;
                    }
    
                    for (size_t k = 0; k < matches.size();) {
                        size_t start   = matches[k];
                        size_t run_len = 1;
    
                        while (k + run_len < matches.size() &&
                               matches[k + run_len] == matches[k + run_len - 1] + 1)
                            ++run_len;
    
                        std::memcpy(dst_val + out,
                                    src_val + start,
                                    run_len * sizeof(T));
    
                        std::copy_n(val_tmp2.begin() + start,
                                    run_len,
                                    val_tmp.begin() + out);
    
                        out += run_len;
                        k   += run_len;
                    }
                }
            }
        }
    };

    #pragma omp parallel for num_threads(outer_threads) schedule(static)
    for (ptrdiff_t t = 0; t < static_cast<ptrdiff_t>(matr_idx1[0].size()); ++t) {
        size_t dst_col = matr_idx1[0][t];
        std::vector<std::string>& val_tmp  = tmp_val_refv[dst_col];
        const std::vector<std::string>& val_tmp2 = tmp_val_refv1[dst_col];
    
        auto*       dst_val = str_v.data()  + nrow  * t;
        const auto* src_val = str_v1.data() + nrow1 * t;
   
        if constexpr (Nested) {
            #pragma omp parallel for num_threads(inner_threads) schedule(static)
            for (ptrdiff_t i_ref = 0; i_ref < static_cast<ptrdiff_t>(nrow1); ++i_ref) {
                const size_t base = out_offset[i_ref];
                const size_t repeat = rep_v[i_ref];
                const std::string& v1 = src_val[i_ref];
                const std::string& v2 = val_tmp2[i_ref];
                for (size_t r = 0; r < repeat; ++r) {
                    dst_val[base + r] = v1;
                    val_tmp[base + r] = v2;
                }
            }
        } else {
            size_t out = 0;
            for (size_t i_ref = 0; i_ref < nrow1; ++i_ref) {
                const size_t repeat = rep_v[i_ref];
            
                const std::string& v1 = src_val[i_ref];
                const std::string& v2 = val_tmp2[i_ref];
            
                for (size_t r = 0; r < repeat; ++r, ++out) {
                    dst_val[out] = v1;
                    val_tmp[out] = v2;
                }
            }
        }
    }

    #pragma omp parallel for num_threads(outer_threads) schedule(static)
    for (ptrdiff_t t = 0; t < static_cast<ptrdiff_t>(matr_idx2b[0].size()); ++t) {
        size_t dst_col = matr_idx2b[0][t];
        size_t src_col = matr_idx2[0][t];

        std::vector<std::string>& val_tmp  = tmp_val_refv[dst_col];
        const std::vector<std::string>& val_tmp2 = tmp_val_refv2[src_col];

        auto*       dst_val = str_v.data()  + nrow  * (size_str1 + t);
        const auto* src_val = str_v2.data() + nrow2 * t;

        if constexpr (Nested) {
            #pragma omp parallel for num_threads(inner_threads) schedule(static)
            for (ptrdiff_t i_ref = 0; i_ref < static_cast<ptrdiff_t>(nrow1); ++i_ref) {
                size_t out = out_offset[i_ref];
                const auto& matches = match_idx[i_ref]; 

                if (!matches.empty()) {
                    for (size_t j_idx : matches) {
                        dst_val[out] = src_val[j_idx];
                        val_tmp[out] = val_tmp2[j_idx];
                        ++out;
                    }
                } else {
                    ++out;
                }
            }
        } else if constexpr (!Nested) {
            size_t out = 0;
            for (size_t i_ref = 0; i_ref < nrow1; ++i_ref) {
                const auto& matches = match_idx[i_ref]; 

                if (!matches.empty()) {
                    for (size_t j_idx : matches) {
                        dst_val[out] = src_val[j_idx];
                        val_tmp[out] = val_tmp2[j_idx];
                        ++out;
                    }
                } else {
                    ++out;
                }
            }
        }
    }

    expand_repeats(chr_v, chr_v1, matr_idx1[1], tmp_val_refv, tmp_val_refv1);
    expand_matches(chr_v, chr_v2,
                   matr_idx2b[1], matr_idx2[1],
                   tmp_val_refv, tmp_val_refv2,
                   size_chr1);

    expand_repeats(bool_v, bool_v1, matr_idx1[2], tmp_val_refv, tmp_val_refv1);
    expand_matches(bool_v, bool_v2,
                   matr_idx2b[2], matr_idx2[2],
                   tmp_val_refv, tmp_val_refv2,
                   size_bool1);

    expand_repeats(int_v, int_v1, matr_idx1[3], tmp_val_refv, tmp_val_refv1);
    expand_matches(int_v, int_v2,
                   matr_idx2b[3], matr_idx2[3],
                   tmp_val_refv, tmp_val_refv2,
                   size_int1);
    
    expand_repeats(uint_v, uint_v1, matr_idx1[4], tmp_val_refv, tmp_val_refv1);
    expand_matches(uint_v, uint_v2,
                   matr_idx2b[4], matr_idx2[4],
                   tmp_val_refv, tmp_val_refv2,
                   size_uint1);
    
    expand_repeats(dbl_v, dbl_v1, matr_idx1[5], tmp_val_refv, tmp_val_refv1);
    expand_matches(dbl_v, dbl_v2,
                   matr_idx2b[5], matr_idx2[5],
                   tmp_val_refv, tmp_val_refv2,
                   size_dbl1);

    type_refv.insert(type_refv.end(), vec_type1.begin(), vec_type1.end());
    type_refv.insert(type_refv.end(), vec_type2.begin(), vec_type2.end());

    ncol += (ncol1 + ncol2);

    const std::vector<std::string>& colname1 = obj_l.get_colname();
    const std::vector<std::string>& colname2 = obj_r.get_colname();

    if (colname2.size() > 0 && colname1.size() > 0) {
      name_v.insert(name_v.end(), colname1.begin(), colname1.end());
      name_v.insert(name_v.end(), colname2.begin(), colname2.end());
    } else {
      name_v.resize(ncol);
    };

    omp_set_nested(prev_nested);
    omp_set_max_active_levels(prev_max_levels);

};




