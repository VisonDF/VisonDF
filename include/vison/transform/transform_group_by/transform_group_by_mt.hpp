#pragma once

template <unsigned int CORES = 4, bool SimdHash = true>
void transform_group_by_mt(std::vector<unsigned int>& x,
                           std::string sumcolname = "n") 
{
    using map_t = std::conditional_t<
        SimdHash,
        ankerl::unordered_dense::map<std::string, unsigned int, simd_hash>,
        ankerl::unordered_dense::map<std::string, unsigned int>
    >;

    const size_t n_threads = CORES;
    const size_t rows_per_thread = (nrow + n_threads - 1) / n_threads;

    std::vector<map_t> thread_maps(n_threads);
    std::vector<std::string> key_vec(nrow); 

    #pragma omp parallel num_threads(n_threads)
    {
        const int tid = omp_get_thread_num();
        map_t& local = thread_maps[tid];
        local.reserve(rows_per_thread / 1.5);

        std::string key;
        key.reserve(128);

        #pragma omp for schedule(static)
        for (size_t i = 0; i < nrow; ++i) {
            key.clear();
            for (size_t j = 0; j < x.size(); ++j) {
                key += tmp_val_refv[x[j]][i];
                key += '\x1F';
            }

            ++local[key];
            key_vec[i] = key; 
        }
    }

    map_t lookup;
    lookup.reserve(nrow / 1.5);
    for (auto& local : thread_maps)
        for (auto& [key, count] : local)
            lookup[key] += count;

    std::vector<unsigned int> occ_v(nrow);
    std::vector<std::string> occ_v_str(nrow);

    #pragma omp parallel for schedule(static)
    for (size_t i = 0; i < nrow; ++i) {
        const std::string& key = key_vec[i];
        unsigned int count = lookup[key];
        occ_v[i] = count;

        char buf[max_chars_needed<unsigned int>()];
        auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), count);
        occ_v_str[i].assign(buf, ptr);
    }

    uint_v.insert(uint_v.end(), occ_v.begin(), occ_v.end());
    tmp_val_refv.push_back(std::move(occ_v_str));

    if (!name_v.empty())
        name_v.push_back(sumcolname);

    type_refv.push_back('u');
    ++ncol;
}

