#pragma once

template <class T, 
          unsigned int CORES = 4>
inline void permute_block_mt(
    std::vector<T>& storage,                               
    std::vector<std::vector<std::string>>& tmp_val_refv,   
    const std::vector<unsigned int>& matr_idx_k,           
    const std::vector<size_t>& idx,
    size_t nrow)
{

    if (matr_idx_k.empty())
        return;

    std::vector<T> tmp_storage(storage.size());
    for (auto& el : tmp_storage) el.resize(nrow);

    std::vector<std::pair<unsigned int, unsigned int>> matr_idx_pair(matr_idx_k.size());
    size_t i3 = 0;

    #pragma unroll
    for (const auto& el : matr_idx_k)  {
        matr_idx_pair[i3] = std::make_pair(el, i3);
        i3 += 1;
    }

    #pragma omp parallel for if(CORES > 1) num_threads(CORES) schedule(static)
    for (size_t i = 0; i < matr_idx_pair.size(); ++i) { 
        
        const auto& [el, i2] = matr_idx_pair[i];
        auto& ref_row = tmp_val_refv[el];

        int thread_id = omp_get_thread_num();

        auto* dst_all = tmp_storage[i2].data();
        auto* src_all = storage[i2].data();

        for (size_t r = 0; r < nrow; ++r) {
            dst_all[r] = src_all[idx[r]];
        }

        ref_row.swap(local_results);
    }

    storage.swap(tmp_storage);
}


