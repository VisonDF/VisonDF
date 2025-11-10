#pragma once

template <unsigned int CORES = 4,
          bool MemClean = false, 
          bool SimdHash = true>
void transform_unique_mt(unsigned int& n) {

    const unsigned int nrow2 = nrow;
    const std::vector<std::string>& val_tmp = tmp_val_refv[n];

    using fast_str_set_t = std::conditional_t<
        SimdHash,
        ankerl::unordered_dense::set<std::string_view, simd_hash>,
        ankerl::unordered_dense::set<std::string_view>
    >;

    std::vector<uint8_t> mask(nrow2, 0);
    std::vector<fast_str_set_t> local_sets(CORES);

    for (auto& set : local_sets)
        set.reserve(nrow2 / CORES);

    #pragma omp parallel num_threads(CORES)
    {
        const unsigned int tid = omp_get_thread_num();
        const size_t chunk_size = (nrow2 + CORES - 1) / CORES;
        const size_t start = tid * chunk_size;
        const size_t end = std::min(start + chunk_size, (size_t)nrow2);
        auto& local_set = local_sets[tid];

        for (size_t i = start; i < end; ++i) {
            std::string_view val = val_tmp[i];
            if (local_set.insert(val).second) {
                mask[i] = 1;  
            }
        }
    }

    fast_str_set_t global_set;
    global_set.reserve(nrow2);

    for (size_t i = 0; i < nrow2; ++i) {
        if (!mask[i]) continue;
        std::string_view val = val_tmp[i];
        if (!global_set.insert(val).second) {
            mask[i] = 0;
        }
    }

    this->transform_filter_mt<CORES, MemClean>(mask);

}



