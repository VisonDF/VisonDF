
inline void parse_rows_chunk_warmed(
    std::string_view local_view,        
    const char*      orig_base,         
    size_t           orig_start_byte,   
    std::vector<std::vector<std::string_view>>& columns,
    char delim, char str_context, unsigned int ncol
) noexcept
{
    const char* base_local = local_view.data();
    const size_t N = local_view.size();
    size_t pos = 0;                    
    size_t field_start = 0;
    bool in_quotes = false;
    size_t verif_ncol = 0;

    const __m256i NL = _mm256_set1_epi8('\n');
    const __m256i CR = _mm256_set1_epi8('\r');
    __m256i D = _mm256_set1_epi8(delim);
    __m256i Q = _mm256_set1_epi8(str_context);

    auto emit_field = [&](size_t start, size_t end) {
        size_t len = end - start;
        const char* emit_ptr = orig_base + orig_start_byte + start; 
        std::string_view field(emit_ptr, len);
        columns[verif_ncol].emplace_back(field.empty() ? std::string_view("NA") : field);
    };

    for (; pos + 32 <= N; ) {
        _mm_prefetch(base_local + pos + 512, _MM_HINT_T0);
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(base_local + pos));
        int mD  = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, D));
        int mQ  = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, Q));
        int mNL = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, NL));
        int mCR = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, CR));
        int mNL_any = (mNL | mCR);
        int events  = (mD | mNL_any | mQ);

        while (events) {
            int bit = __builtin_ctz(events);
            size_t idx = pos + bit;
            char c = base_local[idx];

            if (c == str_context) {
                in_quotes = !in_quotes;
            } else if (!in_quotes && c == delim) {
                emit_field(field_start, idx);
                ++verif_ncol;
                field_start = idx + 1;
            } else if (!in_quotes && (c == '\n' || c == '\r')) {
                emit_field(field_start, idx);
                if (verif_ncol + 1 != ncol) { std::cerr << "column number problem\n"; return; }
                verif_ncol = 0;
                size_t adv = (c == '\r' && idx + 1 < N && base_local[idx + 1] == '\n') ? 2 : 1;
                field_start = idx + adv;
                pos = idx + adv;
                goto next_chunk;
            }
            events &= (events - 1);
        }
        pos += 32;
        next_chunk: continue;
    }

    for (; pos < N; ++pos) {
        char c = base_local[pos];
        if (c == str_context) {
            in_quotes = !in_quotes;
        } else if (!in_quotes && c == delim) {
            emit_field(field_start, pos);
            ++verif_ncol;
            field_start = pos + 1;
        } else if (!in_quotes && (c == '\n' || c == '\r')) {
            emit_field(field_start, pos);
            if (verif_ncol + 1 != ncol) { std::cerr << "column number problem in readf\n"; return; }
            verif_ncol = 0;
            if (c == '\r' && pos + 1 < N && base_local[pos + 1] == '\n') ++pos;
            field_start = pos + 1;
        }
    }
    if (field_start < N) {
        emit_field(field_start, N);
    }
}


