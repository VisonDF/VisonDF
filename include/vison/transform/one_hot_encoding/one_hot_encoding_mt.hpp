#pragma once

template <typename T = void,
          unsigned int CORES = 4,
          bool SimdHash = true>
void one_hot_encoding_mt(unsigned int x)
{

    const unsigned int local_nrow = nrow;
    using variant_t = std::variant<std::string,
                                    CharT,
                                    uint8_t,
                                    IntT,
                                    UIntT,
                                    FloatT>;

    using value_t = std::conditional_t<!(std::is_same_v<T, void>),
                                        T,
                                        variant_t>;

    using set_t = std::conditional_t<SimdHash,
                ankerl::unordered_dense::set<value_t, simd_hash>,
                ankerl::unordered_dense::set<value_t>
                >;

    using container_t = std::variant<
                                    const std::vector<std::vector<std::string>>*,
                                    const std::vector<std::vector<CharT>>*,
                                    const std::vector<std::vector<uint8_t>>*,
                                    const std::vector<std::vector<IntT>>*,
                                    const std::vector<std::vector<UIntT>>*,
                                    const std::vector<std::vector<FloatT>>*,
                                    std::nullptr_t
                                    >;

    container_t key_table = nullptr;
    size_t idx_type;
    if constexpr (!std::is_same_v<T, void>) {
        if constexpr (std::is_same_v<T, std::string>) {
            key_table = &str_v;
            idx_type = 0;
        } else if constexpr (std::is_same_v<T, CharT>) {
            key_table = &chr_v;
            idx_type = 1;
        } else if constexpr (std::is_same_v<T, uint8_t>) {
            key_table = &bool_v;
            idx_type = 2;
        } else if constexpr (std::is_same_v<T, IntT>) {
            key_table = &int_v;
            idx_type = 3;
        } else if constexpr (std::is_same_v<T, UIntT>) {
            key_table = &uint_v;
            idx_type = 4;
        } else if constexpr (std::is_same_v<T, FloatT>) {
            key_table = &dbl_v;
            idx_type = 5;
        }
    } else {
        switch (type_refv[x]) {
            case 's': key_table = &str_v;  idx_type = 0; break;
            case 'c': key_table = &chr_v;  idx_type = 1; break;
            case 'b': key_table = &bool_v; idx_type = 2; break;
            case 'i': key_table = &int_v;  idx_type = 3; break;
            case 'u': key_table = &uint_v; idx_type = 4; break;
            case 'd': key_table = &dbl_v;  idx_type = 5; break;
        }
    }

    std::unordered_map<unsigned int, unsigned int> pos;
    for (int i = 0; i < matr_idx[idx_type].size(); ++i)
        pos[matr_idx[idx_type][i]] = i;

    cons unsigned int real_pos = pos[x];

    auto get_key_col = [&](auto ptr) -> const std::vector<value_t>& {
        return (*ptr)[real_pos];
    };
    const std::vector<value_t>& key_col = std::visit(get_key_col, key_table);

    unsigned int n_unique;
    if constexpr (CORES == 1) {
        
        set_t cur_set;
        cur_set.reserve(local_nrow / 5);
        for (auto& el : key_col)
            cur_set.try_emplace(el);

        n_unique = cur_set.size();

    } else {

        const unsigned int chunks = local_nrow / CORES + 1;
        std::vector<set_t> set_vec(CORES);

        #pragma omp parallel num_threads(CORES)
        {

            const unsigned int tid   = omp_get_thread_num();
            set_t& cur_set = set_vec[tid];
            cur_set.reserve(local_nrow / CORES / 5);
            const unsigned int start = tid * chunks;
            const unsigned int end   = std::min(local_nrow, (start + chunks));

            for (size_t i = start; i < end; ++i)
                cur_set.try_emplace(key_col[i]);

        }

        set_t final_set;
        final_set.reserve(local_nrow / 5);
        for (auto& cur_set : set_vec)
            for (auto& el : cur_set)
                final_set.try_emplace(el);

        n_unique = final_set.size();

    }

    ankerl::unordered_dense::map<value_t, unsigned int> hash_col;
    hash_col.reserve(n_unique);
    {
        size_t i = 0;
        for (auto& el : final_set) {
            hash_col[el] = i;
            i += 1;
        }
    }

    std::vector<std::vector<uint8_t>> cols_to_add(n_unique), std::vector<uint8_t>(local_nrow, 0);
    #pragma omp parallel for if(CORES > 1) num_threads(CORES)
    for (size_t i = 0; i < local_nrow; ++i) {
        const unsigned int idx_col = hash_col[key_col[i]];
        cols_to_add[idx_col][i] = 1;
    }

    type_ref_v.resize(ncol + n_unique, 'b');
    bool_v.reserve(bool_v.size() + n_unique);
    for (size_t i = 0; i < n_unique; ++i)
        bool_v.push_back(cols_to_add[i]);

}





