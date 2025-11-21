struct ColumnResult {
    char type;
    std::vector<std::string> str_v;
    std::vector<char> chr_v;
    std::vector<bool> bool_v;
    std::vector<IntT> int_v;
    std::vector<UIntT> uint_v;
    std::vector<FloatT> dbl_v;
    std::vector<unsigned int> matr_idx[6];
};

template <char TrailingChar = '0'>
inline ColumnResult classify_column(
    const std::vector<std::string>& col_values,
    unsigned int col_idx,
    unsigned int nrow)
{
    ColumnResult result;
    bool is_bool = true;
    bool is_unsigned = true;
    bool is_nb = true;
    bool is_bufchar = true;

    size_t check_count = std::min<size_t>(col_values.size(), 10);

    for (size_t i = 0; i < check_count; ++i) {
        const std::string_view& s = col_values[i];

        if (!simd_can_be_nb(s.data(), s.size())) {
            is_nb = false;
            if (s.size() > df_charbuf_size) {
                is_bufchar = false;
                break;
            }
            continue;
        }

        if (has_dot(s)) {
            result.matr_idx[5].push_back(col_idx);
            result.dbl_v.reserve(nrow);
            for (const auto& el : col_values) {
                FloatT val;
                auto [ptr, ec] = fast_float::from_chars(el.data(), el.data() + el.size(), val);
                result.dbl_v.push_back(ec == std::errc() ? val : static_cast<FloatT>(0));
            }
            result.type = 'd';
            return result;
        }

        if (is_unsigned && !s.empty() && s[0] == '-') {
            is_unsigned = false;
            result.matr_idx[3].push_back(col_idx);
            result.int_v.reserve(nrow);
            for (const auto& el : col_values) {
                IntT val;
                auto [ptr, ec] = std::from_chars(el.data(),
                                                 el.data() + el.size(), val);
                result.int_v.push_back(ec == std::errc() ? val : 0);
            }
            result.type = 'i';
            return result;
        }

        // check if it looks boolean
        if (s != "0" && s != "1")
            is_bool = false;
    }

    // After scanning 10 rows → decide final type

    if (!is_nb) {
        if (!is_bufchar) {
            result.matr_idx[0].push_back(col_idx);
            result.str_v.reserve(nrow);
            result.str_v.insert(result.str_v.end(),
                                col_values.begin(), col_values.end());
            result.type = 's';
            return result;
        } else {
            result.matr_idx[1].push_back(col_idx);
            result.chr_v.reserve(nrow);
            for (const auto& el : col_values) {
                char buf[df_charbuf_size];
                const size_t len = el.size();
                std::memcpy(buf, el.data(), len);
                std::memcpy(buf + len, TrailingChar, df_charbuf_size - len);
                result.chr_v.emplace_back(buf);
            }
            result.type = 'c';
            return result;
        }
    }

    if (is_bool) {
        result.matr_idx[2].push_back(col_idx);
        result.bool_v.reserve(nrow);
        for (const auto& el : col_values) {
            int tmp;
            auto [ptr, ec] = std::from_chars(el.data(),
                                             el.data() + el.size(), tmp);
            result.bool_v.push_back(ec == std::errc() ? (tmp != 0) : false);
        }
        result.type = 'b';
    } else if (is_unsigned) {
        result.matr_idx[4].push_back(col_idx);
        result.uint_v.reserve(nrow);
        for (const auto& el : col_values) {
            UIntT val;
            auto [ptr, ec] = std::from_chars(el.data(),
                                             el.data() + el.size(), val);
            result.uint_v.push_back(ec == std::errc() ? val : 0u);
        }
        result.type = 'u';
    } else {
        result.matr_idx[3].push_back(col_idx);
        result.int_v.reserve(nrow);
        for (const auto& el : col_values) {
            IntT val;
            auto [ptr, ec] = std::from_chars(el.data(),
                                             el.data() + el.size(), val);
            result.int_v.push_back(ec == std::errc() ? val : 0);
        }
        result.type = 'i';
    }

    return result;
}


