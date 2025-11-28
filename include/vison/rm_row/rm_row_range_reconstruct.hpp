#pragma once

template <bool MemClean = false, bool LowDensity = true>
void rm_row_range_reconstruct(std::vector<unsigned int> x)
{
    const size_t old_nrow = nrow;
    if (x.empty() || old_nrow == 0) return;

    std::vector<uint8_t> keep(old_nrow, 1);
    for (unsigned rr : x) if (rr < old_nrow) keep[rr] = 0;

    const size_t new_nrow = old_nrow - x.size();

    std::vector<std::string> new_str_v;  new_str_v .reserve(matr_idx[0].size() * new_nrow);
    std::vector<char>        new_chr_v;  new_chr_v .reserve(matr_idx[1].size() * new_nrow);
    std::vector<uint8_t>     new_bool_v; new_bool_v.reserve(matr_idx[2].size() * new_nrow);
    std::vector<IntT>        new_int_v;  new_int_v .reserve(matr_idx[3].size() * new_nrow);
    std::vector<UIntT>       new_uint_v; new_uint_v.reserve(matr_idx[4].size() * new_nrow);
    std::vector<FloatT>      new_dbl_v;  new_dbl_v .reserve(matr_idx[5].size() * new_nrow);

    std::vector<std::vector<std::string>> new_tmp_val_refv(tmp_val_refv.size());
    std::vector<std::string> new_name_v_row;
    if (!name_v_row.empty()) {
        new_name_v_row.reserve(new_nrow);
        for (size_t i = 0; i < old_nrow; ++i)
            if (keep[i]) new_name_v_row.push_back(std::move(name_v_row[i]));
    }

    auto compact_block_pod = [&]<typename T>(std::vector<T>& dst, 
                    const std::vector<T>& src, size_t base) {
        const char* s = src.data() + base;
        const size_t base_out = dst.size();
        size_t written = 0, i = 0;
        dst.reserve(base_out + new_nrow);
        while (i < old_nrow) {
            while (i < old_nrow && !keep[i]) ++i;
            const size_t start = i;
            while (i < old_nrow && keep[i]) ++i;
            const size_t len = i - start;
            if (len) {
                dst.resize(base_out + written + len);
                std::memcpy(dst.data() + base_out + written, s + start, len * sizeof(T));
                written += len;
            }
        }
    };

    auto compact_block_low_dense = [&](auto& dst, const auto& src, size_t base) {
        auto beg = src.begin() + base;
        dst.reserve(dst.size() + new_nrow);
        size_t i = 0;
        while (i < old_nrow) {
            while (i < old_nrow && !keep[i]) ++i;
            const size_t start = i;
            while (i < old_nrow && keep[i]) ++i;
            const size_t len = i - start;
            if (len) dst.insert(dst.end(), beg + start, beg + start + len);
        }
    };

    auto compact_block_scalar = [&](auto& dst, const auto& src, size_t base) {
        auto beg = src.begin() + base;
        dst.reserve(dst.size() + new_nrow);
        for (size_t i = 0; i < old_nrow; ++i)
            if (keep[i]) dst.push_back(*(beg + i));
    };

    for (size_t t = 0; t < 6; ++t) {
        const auto& idx = matr_idx[t];
        const size_t ncols_t = idx.size();
        if (ncols_t == 0) continue;

        for (size_t cpos = 0; cpos < ncols_t; ++cpos) {
            const size_t base = cpos * old_nrow;

            switch (t) {
                case 0: 
                    if constexpr (LowDensity)
                        compact_block_low_dense(new_str_v,  str_v,  base);
                    else
                        compact_block_scalar   (new_str_v,  str_v,  base);
                    break;

                case 1: 
                    compact_block_pod.template operator()<CharT>(new_chr_v,  chr_v,  base);
                    break;

                case 2: 

                    compact_block_pod.template operator()<uint8_t>(new_bool_v,  bool_v,  base);

                    break;

                case 3: 
                    compact_block_pod.template operator()<IntT>(new_int_v,  int_v,  base);
                    break;

                case 4: 
                    compact_block_pod.template operator()<UIntT>(new_uint_v, uint_v, base);
                    break;

                case 5: 
                    compact_block_pod.template operator()<FloatT>(new_dbl_v,  dbl_v,  base);
                    break;
            }

            auto& src_aux = tmp_val_refv[idx[cpos]];
            auto& dst_aux = new_tmp_val_refv[idx[cpos]];
            dst_aux.reserve(src_aux.size());
            for (size_t i = 0; i < old_nrow; ++i)
                if (keep[i]) dst_aux.push_back(std::move(src_aux[i]));

            if constexpr (MemClean) dst_aux.shrink_to_fit();
        }
    }

    str_v.swap(new_str_v);
    chr_v.swap(new_chr_v);
    bool_v.swap(new_bool_v);
    int_v.swap(new_int_v);
    uint_v.swap(new_uint_v);
    dbl_v.swap(new_dbl_v);
    tmp_val_refv.swap(new_tmp_val_refv);
    name_v_row.swap(new_name_v_row);

    nrow = new_nrow;

    if constexpr (MemClean) {
        for (auto& v : tmp_val_refv) v.shrink_to_fit();
        str_v.shrink_to_fit(); chr_v.shrink_to_fit(); bool_v.shrink_to_fit();
        int_v.shrink_to_fit(); uint_v.shrink_to_fit(); dbl_v.shrink_to_fit();
    }
}


