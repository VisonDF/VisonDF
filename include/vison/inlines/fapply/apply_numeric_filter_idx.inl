#pragma once

template <bool MapCol = false,
          unsigned int CORES = 4,
          typename T, 
          typename F>
inline void apply_numeric_filter_idx(const std::vector<T>& values, 
                                     const unsigned int n, 
                                     const size_t idx_type, 
                                     F&& f,
                                     const std::vector<unsigned int>& mask) 
{

    unsigned int i2 = 0;
    if constexpr (!MapCol) {
        i2 = 0;
        while (i2 < matr_idx[idx_type].size()) {
            if (n == matr_idx[idx_type][i2])
                break;
            ++i2;
        }
    } else {
        if (!matr_idx_map[idx_type].contains(n)) {
            std::cerr << "MapCol used but col not found in the map\n";
            return;
        }
        if (!sync_map_col[idx_type]) {
            std::cerr << "Col not synced\n";
            return;
        }

        i2 = matr_idx_map[idx_type][n];

    }


    std::vector<T>& dst = values[i2];
    
    //#if defined(__clang__)
    //    #pragma clang loop vectorize(enable)
    //#elif defined(__GNUC__)
    //    #pragma GCC ivdep
    //#elif defined(_MSC_VER)
    //    #pragma loop(ivdep)
    //#endif

    #pragma omp parallel for if(CORES > 1) num_threads(CORES)
    for (size_t i = 0; i < mask.size(); ++i) {
        f(dst[mask[i]]);
    }

}




