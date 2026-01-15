#pragma once

template <unsigned int CORES = 1, 
          char TrailingChar = '0',
          bool NoInfer = false>
void type_classification(std::vector<char> dtype = {}) {
    
    type_refv.resize(ncol);                

    for (auto& el : matr_idx) el.reserve(ncol);

    std::vector<ColumnResult> results(ncol);

    if constexpr (!NoInfer) {
        #pragma omp parallel for num_threads(CORES)
        for (size_t i = 0; i < ncol; ++i) {
            results[i] = classify_column<TrailingChar>(tmp_val_refv[i], i, nrow);
        }
    } else if constexpr (NoInfer) {
        #pragma omp parallel for num_threads(CORES)
        for (size_t i = 0; i < ncol; ++i) {
            switch(dtype[i]) {
                case 's': {
                              results[i].matr_idx[0].push_back(i);
                              results[i].type = 's';
                              std::vector<std::string>& dst = results[i].str_v;
                              dst.resize(nrow);
                              const std::vector<std::string>& src = tmp_val_refv[i];
                              for (size_t i2 = 0; i2 < src.size(); ++i2) {
                                  dst[i2] = src[i2];
                              }
                          }
                case 'c': {
                              results[i].matr_idx[1].push_back(i);
                              results[i].type = 'c';
                              std::vector<std::string>& dst = results[i].chr_v;
                              dst.resize(nrow);
                              const std::vector<CharT>& src = tmp_val_refv[i];
                              for (size_t i2 = 0; i2 < src.size(); ++i2) {
                                  char buf[df_charbuf_size];
                                  const std::string& el = src[i2];
                                  const size_t len = el.size();
                                  std::memcpy(buf, el.data(), len);
                                  std::memcpy(buf + len, TrailingChar, df_charbuf_size - len);
                                  dst[i2].assign(buf, df_charbuf_size);
                              }
                          }
                case 'b': {
                              results[i].matr_idx[2].push_back(i);
                              results[i].type = 'b';
                              std::vector<uint8_t>& dst = results[i].bool_v;
                              dst.resize(nrow);
                              const std::vector<uint8_t>& src = tmp_val_refv[i];
                              for (size_t i2 = 0; i2 < src.size(); ++i2) {
                                  int val;
                                  const std::string& el = src[i2];
                                  auto [ptr, ec] = std::from_chars(el.data(),
                                                                   el.data() + el.size(), val);
                                  dst[i2] = (ec == std::errc() ? (val != 0) : false);
                              }
                          }
                case 'i': {
                              results[i].matr_idx[3].push_back(i);
                              results[i].type = 'i';
                              std::vector<IntT>& dst = results[i].int_v;
                              dst.resize(nrow);
                              const std::vector<IntT>& src = tmp_val_refv[i];
                              for (size_t i2 = 0; i2 < src.size(); ++i2) {
                                  IntT val;
                                  const std::string& el = src[i2];
                                  auto [ptr, ec] = std::from_chars(el.data(),
                                                                   el.data() + el.size(), val);
                                  dst[i2] = (ec == std::errc() ? val : 0);
                              }
                          }
                case 'u': {
                              results[i].matr_idx[4].push_back(i);
                              results[i].type = 'u';
                              std::vector<UIntT>& dst = results[i].uint_v;
                              dst.resize(nrow);
                              const std::vector<IntT>& src = tmp_val_refv[i];
                              for (size_t i2 = 0; i2 < src.size(); ++i2) {
                                  UIntT val;
                                  const std::string& el = src[i2];
                                  auto [ptr, ec] = std::from_chars(el.data(),
                                                                   el.data() + el.size(), val);
                                  dst[i2] = (ec == std::errc() ? val : 0);
                              }
                          }
                case 'd': {
                              results[i].matr_idx[5].push_back(i);
                              results[i].type = 'd';
                              std::vector<FloatT>& dst = results[i].dbl_v;
                              dst.resize(nrow);
                              const std::vector<IntT>& src = tmp_val_refv[i];
                              for (size_t i2 = 0; i2 < src.size(); ++i2) {
                                  FloatT val;
                                  const std::string& el = src[i2];
                                  //auto [ptr, ec] = std::from_chars(el.data(),
                                  //                                 el.data() + el.size(), val);
                                  auto [ptr, ec] = fast_float::from_chars(el.data(), el.data() + el.size(), val);
                                  dst[i2] = (ec == std::errc() ? val : static_cast<FloatT>(0));
                              }
                          }

            }
        }
    }
    
    size_t total_str=0, total_chr=0, total_bool=0, total_int=0, total_uint=0, total_dbl=0;
    std::array<size_t, 6> total_idx{0,0,0,0,0,0};

    std::vector<size_t> off_str(results.size(),  0),
                        off_chr(results.size(),  0),
                        off_bool(results.size(), 0),
                        off_int(results.size(),  0),
                        off_uint(results.size(), 0),
                        off_dbl(results.size(),  0);

    size_t cnt = 0;
    for (const auto& r : results) {
        
        if (r.str_v.size() > 0)  { off_str[cnt]   = total_str ; total_str  += 1; };
        if (r.chr_v.size() > 0)  { off_chr[cnt]   = total_chr ; total_chr  += 1; };
        if (r.bool_v.size() > 0) { off_bool[cnt]  = total_bool; total_bool += 1; };
        if (r.int_v.size() > 0)  { off_int[cnt]   = total_int ; total_int  += 1; };
        if (r.uint_v.size() > 0) { off_uint[cnt]  = total_uint; total_uint += 1; };
        if (r.dbl_v.size() > 0)  { off_dbl[cnt]   = total_dbl;  total_dbl  += 1; };

        for (size_t t = 0; t < 6; ++t) total_idx[t] += r.matr_idx[t].size();
        cnt += 1;
    }

    str_v.resize (total_str);
    chr_v.resize (total_chr);
    bool_v.resize(total_bool);
    int_v.resize (total_int);
    uint_v.resize(total_uint);
    dbl_v.resize (total_dbl);
    for (auto& el : str_v ) { el.resize(nrow) };
    for (auto& el : chr_v ) { el.resize(nrow) };
    for (auto& el : bool_v) { el.resize(nrow) };
    for (auto& el : int_v ) { el.resize(nrow) };
    for (auto& el : uint_v) { el.resize(nrow) };
    for (auto& el : dbl_v ) { el.resize(nrow) };
    for (size_t t = 0 ; t < 6; ++t) matr_idx[t].resize(total_idx[t]);

    std::array<std::vector<size_t>,6> off_idx;
    for (auto& v : off_idx)
        v.resize(results.size());

    for (size_t i = 1; i < results.size(); ++i) {
        for (size_t t = 0; t < 6; ++t)
            off_idx[t][i] = off_idx[t][i - 1] + results[i - 1].matr_idx[t].size();
    }

    #pragma omp parallel for schedule(static) num_threads(CORES)
    for (size_t i = 0; i < results.size(); ++i) {
        auto& r = results[i];
        type_refv[i] = r.type;

        std::move(r.str_v.begin(),  r.str_v.end(),  str_v [ off_str [i] ].begin());
        std::move(r.chr_v.begin(),  r.chr_v.end(),  chr_v [ off_chr [i] ].begin());
        std::move(r.bool_v.begin(), r.bool_v.end(), bool_v[ off_bool[i] ].begin());
        std::move(r.int_v.begin(),  r.int_v.end(),  int_v [ off_int [i] ].begin());
        std::move(r.uint_v.begin(), r.uint_v.end(), uint_v[ off_uint[i] ].begin());
        std::move(r.dbl_v.begin(),  r.dbl_v.end(),  dbl_v [ off_dbl [i] ].begin());

        for (size_t t = 0;t < 6; ++t)
            std::move(r.matr_idx[t].begin(),
                      r.matr_idx[t].end(),
                      matr_idx[t].begin() + off_idx[t][i]);
    }
}



