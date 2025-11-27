
template <typename ColumnsType, 
          bool Lambda = false, 
          typename F>
inline void parse_rows_chunk(
    std::string_view chunk_view,
    std::vector<std::vector<ColumnsType>>& columns,
    const char delim,
    const char str_context,
    const unsigned int ncol,
    F f
) noexcept
{
    const char* base = chunk_view.data();
    const size_t N = chunk_view.size();

    size_t pos = 0;
    bool in_quotes = false;
    size_t field_start = 0;
    size_t verif_ncol = 0;

    #if defined (__AVX512F__)
    static const __m512i NL = _mm512_set1_epi8('\n');
    static const __m512i CR = _mm512_set1_epi8('\r');
    const __m512i D = _mm512_set1_epi8(delim);
    const __m512i Q = _mm512_set1_epi8(str_context);
    const size_t BATCH = 64;
    #elif defined(__AVX2__)
    static const __m256i NL = _mm256_set1_epi8('\n');
    static const __m256i CR = _mm256_set1_epi8('\r');
    const __m256i D = _mm256_set1_epi8(delim);
    const __m256i Q = _mm256_set1_epi8(str_context);
    const size_t BATCH = 32;
    #endif

    for (; pos + BATCH <= N; ) {

        #if defined (__AVX512F__)
        _mm_prefetch(base + pos + 1024, _MM_HINT_T0);
        __m512i chunk = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(base + pos));

        int64_t mD  = _mm512_movemask_epi8(_mm512_cmpeq_epi8(chunk, D));
        int64_t mQ  = _mm512_movemask_epi8(_mm512_cmpeq_epi8(chunk, Q));
        int64_t mNL = _mm512_movemask_epi8(_mm512_cmpeq_epi8(chunk, NL));
        int64_t mCR = _mm512_movemask_epi8(_mm512_cmpeq_epi8(chunk, CR));

        int64_t mNL_any = (mNL | mCR);
        int64_t events  = (mD | mNL_any | mQ);

        #elif defined (__AVX2__)
        _mm_prefetch(base + pos + 512, _MM_HINT_T0);
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(base + pos));

        int32_t mD  = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, D));
        int32_t mQ  = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, Q));
        int32_t mNL = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, NL));
        int32_t mCR = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, CR));

        int32_t mNL_any = (mNL | mCR);
        int32_t events  = (mD | mNL_any | mQ);

        #endif

        while (events) {
            // get the index of the least significant bit equals to 1
            // in fact the first matching bit (order reversed - right to left)
            #if defined (__AVX512F__)
            int bit = __builtin_ctzll(events);
            #elif defined (__AVX2__)
            int bit = __builtin_ctz(events);
            #endif

            size_t idx = pos + bit;
            char c = base[idx];

            if (c == str_context) {
                in_quotes = !in_quotes;
            }
            else if (!in_quotes && c == delim) {
                std::string_view field = chunk_view.substr(field_start, idx - field_start);
                auto& col = columns[verif_ncol];
                
                if (field.empty()) [[unlikely]] {
                    col.emplace_back("NA");
                } else if constexpr (!Lambda) {
                    col.emplace_back(field);
                } else {
                    col.emplace_back(f(field));
                }

                ++verif_ncol;
                field_start = idx + 1;
            }
            else if (!in_quotes && (c == '\n' || c == '\r')) {
                std::string_view field = chunk_view.substr(field_start, idx - field_start);
                auto& col = columns[verif_ncol];
                
                if (field.empty()) [[unlikely]] {
                    col.emplace_back("NA");
                } else if constexpr (!Lambda) {
                    col.emplace_back(field);
                } else {
                    col.emplace_back(f(field));
                }

                if (verif_ncol + 1 != ncol) {
                    std::cerr << "column number problem\n";
                    return;
                }

                verif_ncol = 0;
                size_t advance = (c == '\r' && idx + 1 < N && base[idx + 1] == '\n') ? 2 : 1;
                field_start = idx + advance;
                pos = idx + advance;
                goto next_chunk;
            }

            events &= (events - 1);
        }

        pos += BATCH;
        next_chunk:
          continue;
    }

    for (; pos < N; ++pos) {
        char c = base[pos];
        if (c == str_context) {
            in_quotes = !in_quotes;
        } else if (!in_quotes && c == delim) {
            std::string_view field = chunk_view.substr(field_start, pos - field_start);
            auto& col = columns[verif_ncol];
            
            if (field.empty()) [[unlikely]] {
                col.emplace_back("NA");
            } else if constexpr (!Lambda) {
                col.emplace_back(field);
            } else {
                col.emplace_back(f(field));
            }

            ++verif_ncol;
            field_start = pos + 1;
        } else if (!in_quotes && (c == '\n' || c == '\r')) {
            std::string_view field = chunk_view.substr(field_start, pos - field_start);
            auto& col = columns[verif_ncol];
            
            if (field.empty()) [[unlikely]] {
                col.emplace_back("NA");
            } else if constexpr (!Lambda) {
                col.emplace_back(field);
            } else {
                col.emplace_back(f(field));
            }

            if (verif_ncol + 1 != ncol) {
                std::cerr << "column number problem in readf\n";
                return;
            }

            verif_ncol = 0;

            if (c == '\r' && pos + 1 < N && base[pos + 1] == '\n')
                ++pos;

            field_start = pos + 1;
        }
    }

    if (field_start < N) {
        std::string_view field = chunk_view.substr(field_start, N - field_start);
        auto& col = columns[verif_ncol];
        
        if (field.empty()) [[unlikely]] {
            col.emplace_back("NA");
        } else if constexpr (!Lambda) {
            col.emplace_back(field);
        } else {
            col.emplace_back(f(field));
        }
    }

}


