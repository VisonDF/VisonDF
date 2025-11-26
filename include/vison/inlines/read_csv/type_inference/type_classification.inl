#pragma once

template <unsigned int CORES = 1, char TrailingChar = '0'>
void type_classification() {
    
    type_refv.resize(ncol);                

    for (auto& el : matr_idx) el.reserve(ncol);

    std::vector<ColumnResult> results(ncol);

    #pragma omp parallel for num_threads(CORES)
    for (int i = 0; i < ncol; ++i) {
        results[i] = classify_column<TrailingChar>(tmp_val_refv[i], i, nrow);
    }
    
    size_t total_str=0, total_chr=0, total_bool=0, total_int=0, total_uint=0, total_dbl=0;
    std::array<size_t,6> total_idx{0,0,0,0,0,0};

    for (const auto& r : results) {
        total_str  += r.str_v.size();
        total_chr  += r.chr_v.size();
        total_bool += r.bool_v.size();
        total_int  += r.int_v.size();
        total_uint += r.uint_v.size();
        total_dbl  += r.dbl_v.size();
        for (size_t t = 0; t < 6; ++t) total_idx[t] += r.matr_idx[t].size();
    }

    str_v.resize (total_str);
    chr_v.resize (total_chr);
    bool_v.resize(total_bool);
    int_v.resize (total_int);
    uint_v.resize(total_uint);
    dbl_v.resize (total_dbl);
    for (size_t t = 0 ; t < 6; ++t) matr_idx[t].resize(total_idx[t]);

    std::vector<size_t> off_str(results.size()),
                        off_chr(results.size()),
                        off_bool(results.size()),
                        off_int(results.size()),
                        off_uint(results.size()),
                        off_dbl(results.size());

    std::array<std::vector<size_t>,6> off_idx;
    for (auto& v : off_idx)
        v.resize(results.size());

    for (size_t i = 1; i < results.size(); ++i) {
        off_str[i]  = off_str[i - 1]  + results[i - 1].str_v.size();
        off_chr[i]  = off_chr[i - 1]  + results[i - 1].chr_v.size();
        off_bool[i] = off_bool[i - 1] + results[i - 1].bool_v.size();
        off_int[i]  = off_int[i - 1]  + results[i - 1].int_v.size();
        off_uint[i] = off_uint[i - 1] + results[i - 1].uint_v.size();
        off_dbl[i]  = off_dbl[i - 1]  + results[i - 1].dbl_v.size();
        for (size_t t = 0; t < 6; ++t)
            off_idx[t][i] = off_idx[t][i-1] + results[i-1].matr_idx[t].size();
    }

    #pragma omp parallel for schedule(static) num_threads(CORES)
    for (size_t i = 0; i < results.size(); ++i) {
        auto& r = results[i];
        type_refv[i] = r.type;

        std::move(r.str_v.begin(),  r.str_v.end(),  str_v.begin() + off_str[i]);
        std::move(r.chr_v.begin(),  r.chr_v.end(),  chr_v.begin() + off_chr[i]);
        std::move(r.bool_v.begin(), r.bool_v.end(), bool_v.begin() + off_bool[i]);
        std::move(r.int_v.begin(),  r.int_v.end(),  int_v.begin()  + off_int[i]);
        std::move(r.uint_v.begin(), r.uint_v.end(), uint_v.begin() + off_uint[i]);
        std::move(r.dbl_v.begin(),  r.dbl_v.end(),  dbl_v.begin() + off_dbl[i]);

        for (size_t t = 0;t < 6; ++t)
            std::move(r.matr_idx[t].begin(),
                      r.matr_idx[t].end(),
                      matr_idx[t].begin() + off_idx[t][i]);
    }
}



