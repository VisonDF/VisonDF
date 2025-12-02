#pragma once

template <unsigned int CORES = 4,
          bool OnlyView = false>
void rm_row_range_reconstruct_mt(std::vector<unsigned int>& x)
{
    const size_t old_nrow = nrow;
    if (x.empty() || old_nrow == 0) return;

    const size_t new_nrow = old_nrow - x.size();

    std::vector<std::vector<std::string>> new_tmp_val_refv(tmp_val_refv.size());

    auto compact_block_pod = [&]<typename T>(std::vector<T>& dst, 
                                             std::vector<T>& src) {

        size_t i = 0;
        size_t i2 = 0;
        size_t written = 0;
        while (i2 < x.size()) {
            const unsigned int ref_val = x[i2++];
            const size_t start = i;
            while (i < ref_val) ++i;
            const size_t len = i - start;
            std::memcpy(dst.data() + written, 
                        src.data() + start, 
                        len * sizeof(T));
            written += len;
            i += 1;
        }
        const size_t start = i;
        while (i < old_nrow) ++i;
        const size_t len = i - start;
        std::memcpy(dst.data() + written, 
                    src.data() + start, 
                    len * sizeof(T));
    };

    auto compact_block_scalar = [&](auto& dst, 
                                    auto& src) {
        size_t i = 0;
        size_t i2 = 0;
        size_t written = 0;
        while (i2 < x.size()) {
            const unsigned int ref_val = x[i2++];
            while (i < ref_val) {
                dst[written] = std::move(src[i]);
                i += 1;
                written += 1;
            };
            i += 1;
        }
        while (i < old_nrow) {
            dst[written] = std::move(src[i]);
            i += 1;
            written += 1;
        };
    };

    for (size_t t = 0; t < 6; ++t) {
        
        const auto& idx = matr_idx[t];
        const size_t ncols_t = idx.size();
        if (ncols_t == 0) continue;

        switch (t) {
            case 0: 
                #pragma omp parallel for num_threads(CORES)
                for (size_t cpos = 0; cpos < ncols_t; ++cpos)
                    compact_block_scalar(str_v[cpos], str_v[cpos]);
                break;
            case 1:
                #pragma omp parallel for num_threads(CORES)
                for (size_t cpos = 0; cpos < ncols_t; ++cpos) {
                    compact_block_pod.template operator()<CharT>(chr_v[cpos],  
                                                                 chr_v[cpos]);
                }
                break;
            case 2: 
                #pragma omp parallel for num_threads(CORES)
                for (size_t cpos = 0; cpos < ncols_t; ++cpos) {
                    compact_block_pod.template operator()<uint8_t>(bool_v[cpos],  
                                                                   bool_v[cpos]);
                }
                break;
            case 3:
                #pragma omp parallel for num_threads(CORES)
                for (size_t cpos = 0; cpos < ncols_t; ++cpos) {
                    compact_block_pod.template operator()<IntT>(int_v[cpos], 
                                                                int_v[cpos]);
                }
                break;
            case 4:
                #pragma omp parallel for num_threads(CORES)
                for (size_t cpos = 0; cpos < ncols_t; ++cpos) {
                    compact_block_pod.template operator()<UIntT>(uint_v[cpos], 
                                                                 uint_v[cpos]);
                }
                break;
            case 5:
                #pragma omp parallel for num_threads(CORES)
                for (size_t cpos = 0; cpos < ncols_t; ++cpos) {
                    compact_block_pod.template operator()<FloatT>(dbl_v[cpos],
                                                                  dbl_v[cpos]);
                }
                break;
        }

    }
    
    #pragma omp parallel for num_threads(CORES)
    for (size_t cpos = 0; cpos < ncol; ++cpos) {
        auto& src_aux = tmp_val_refv[cpos];
        auto& dst_aux = tmp_val_refv[cpos];
        size_t i = 0;
        size_t i2 = 0;
        size_t written = 0;
        while (i2 < x.size()) {
            const unsigned int ref_val = x[i2++];
            while (i < ref_val) {
                dst_aux[written] = std::move(src_aux[i]);
                i += 1;
                written += 1;
            };
            i += 1;
        }
        while (i < old_nrow) {
            dst_aux[i] = std::move(src_aux[i]);
            i += 1;
            written += 1;
        };
    }

    std::vector<std::string> new_name_v_row;
    if (!name_v_row.empty()) {
        new_name_v_row.resize(new_nrow);
        size_t i = 0;
        size_t i2 = 0;
        size_t written = 0;
        while (i2 < x.size()) {
            const unsigned int ref_val = x[i2++];
            while (i < ref_val) {
                new_name_v_row[written] = std::move(name_v_row[i]);
                i += 1;
                written += 1;
            };
            i += 1;
        }
        while (i < old_nrow) {
            new_name_v_row[i] = std::move(name_v_row[i]);
            i += 1;
            written += 1;
        };
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

}


