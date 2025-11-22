#pragma once

template <unsigned int CORES = 4, 
          bool Simd = true>
inline size_t max_chars_string_col(const std::string* col) const 
{

    if constexpr (CORES <= 1) {

         size_t max_length = 0;
         for (size_t i = 0; i < nrow; ++i) {
             max_length = std::max(max_length, col[i].length());
         }

         return max_length;

    } else if constexpr (CORES > 1) {

        std::vector<size_t> local_max(CORES, 0);
        
        auto range = [&](int t) {
            size_t chunk = nrow / CORES;
            size_t rem   = nrow % CORES;
            size_t beg   = t * chunk + std::min<size_t>(t, rem);
            size_t end   = beg + chunk + (t < rem ? 1 : 0);
            return std::pair<size_t,size_t>(beg, end);
        };
        
        #pragma omp parallel num_threads(CORES)
        {
            int t = omp_get_thread_num();
            auto [beg, end] = range(t);
        
            size_t m = 0;
            for (size_t i = beg; i < end; ++i) {
                size_t len = col[i].length();
                if (len > m)
                    m = len;
            }
            local_max[t] = m;
        }
        
        size_t max_length = 0;
        for (size_t t = 0; t < CORES; ++t)
            max_length = std::max(max_length, local_max[t]);

        return max_length;

    }

}



