#pragma once

inline void permute_block_bool(
    std::vector<bool>& storage,
    std::vector<std::vector<std::string>>& tmp_val_refv,
    std::vector<std::string>& str_v2,
    const std::vector<unsigned int>& matr_idx_k,
    const std::vector<size_t>& idx,
    size_t nrow)
{
    std::vector<bool> tmp_storage(storage.size());

    size_t i2 = 0;
    for (auto el : matr_idx_k) {
        const size_t pos_vl = i2 * nrow;

        auto& ref_row = tmp_val_refv[el];

        auto* dst_col = str_v2.data();
        auto* src_col = ref_row.data();
        auto dst_all = tmp_storage.begin() + pos_vl;
        auto src_all = storage.begin() + pos_vl;

        for (size_t r = 0; r < nrow; ++r) {
            const size_t pos_vl2 = idx[r];

            dst_col[r] = std::move(src_col[pos_vl2]);   
            dst_all[r] = src_all[pos_vl2];              
        }

        ref_row.swap(str_v2);
        ++i2;
    }

    storage.swap(tmp_storage);
}
