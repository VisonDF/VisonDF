#pragma once

template <unsigned int CORES= 4>
void longest_determine() {

    longest_v.resize(ncol);
    
    if (!name_v.empty()) {
        for (unsigned i = 0; i < ncol; ++i)
            longest_v[i] = name_v[i].size();
    } else {
        std::fill(longest_v.begin(), longest_v.end(), 0);
    }
   
    #pragma omp parallel for num_threads(CORES)
    for (unsigned i = 0; i < ncol; ++i) {
        auto* __restrict col = tmp_val_refv[i].data();
        unsigned max_len = longest_v[i];
    
        #pragma GCC ivdep
        #pragma GCC unroll 8
        for (unsigned row = 0; row < nrow; ++row)
            max_len = std::max<unsigned>(max_len, col[row].size());
    
        longest_v[i] = max_len;
    }

};
