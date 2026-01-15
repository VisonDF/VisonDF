
template <bool Lambda = false, typename F>
inline void parse_rows_chunk_warmed(
    std::string_view local_view,        
    const char*      orig_base,         
    size_t           orig_start_byte,   
    std::vector<std::vector<std::string_view>>& columns,
    char delim, char str_context, 
    unsigned int ncol,
    F f
) noexcept
{
    const char* base_local = local_view.data();
    const size_t N = local_view.size();
    size_t pos = 0;                    
    size_t field_start = 0;
    bool in_quotes = false;
    size_t verif_ncol = 0;

    #if defined (__AVX512F__)
    const __m512i NL = _mm512_set1_epi8('\n');
    const __m512i CR = _mm512_set1_epi8('\r');
    const __m512i D =  _mm512_set1_epi8(delim);
    const __m512i Q =  _mm512_set1_epi8(str_context);
    const size_t BATCH = 64;
    #elif defined (__AVX2__)
    const __m256i NL = _mm256_set1_epi8('\n');
    const __m256i CR = _mm256_set1_epi8('\r');
    const __m256i D =  _mm256_set1_epi8(delim);
    const __m256i Q =  _mm256_set1_epi8(str_context);
    const size_t BATCH = 32;
    #endif

    auto emit_field = [&](size_t start, size_t end) {
        size_t len = end - start;
        const char* emit_ptr = orig_base + orig_start_byte + start; 
        std::string_view field(emit_ptr, len);

        auto& col = columns[verif_ncol];
        
        if (field.empty()) [[unlikely]] {
            col.emplace_back("NA");
        } else if constexpr (!Lambda) {
            col.emplace_back(field);
        } else {
            col.emplace_back(f(field));
        }

    };

    for (; pos + BATCH <= N; ) {
        #if defined (__AVX512F__)
        _mm_prefetch(base_local + pos + 1024, _MM_HINT_T0);
        __m512i chunk = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(base_local + pos));
        int64_t mD  = _mm512_movemask_epi8(_mm512_cmpeq_epi8(chunk, D));
        int64_t mQ  = _mm512_movemask_epi8(_mm512_cmpeq_epi8(chunk, Q));
        int64_t mNL = _mm512_movemask_epi8(_mm512_cmpeq_epi8(chunk, NL));
        int64_t mCR = _mm512_movemask_epi8(_mm512_cmpeq_epi8(chunk, CR));
        // here we just OR the mask
        int64_t mNL_any = (mNL | mCR);
        int64_t events  = (mD | mNL_any | mQ);
        #elif defined (__AVX2__)
        _mm_prefetch(base_local + pos + 512, _MM_HINT_T0);
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(base_local + pos));
        int32_t mD  = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, D));
        int32_t mQ  = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, Q));
        int32_t mNL = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, NL));
        int32_t mCR = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, CR));
        // here we just OR the mask
        int32_t mNL_any = (mNL | mCR);
        int32_t events  = (mD | mNL_any | mQ);
        #endif

        // We have a 32-bit mask where each bit corresponds to one byte in the AVX chunk.
        // Example: events = 000...0111  (three matches in the lowest lanes)
        //
        // Each iteration:
        //      events &= (events - 1);
        //
        // This uses the classic “remove lowest set bit” trick.
        // Visually:
        //
        //   events = 000...0111
        //   events - 1 = 000...0110
        //   ------------------------ AND
        //   result =      000...0110   (lowest 1-bit cleared)
        //
        // Next iteration:
        //   events = 000...0110
        //   events - 1 = 000...0101
        //   AND →       000...0100
        //
        // Next:
        //   events = 000...0100
        //   events - 1 = 000...0011
        //   AND →       000...0000
        //
        // So each pass removes exactly one lowest 1-bit, letting us visit
        // every “true” bit position (character match) in the mask *once*
        // without scanning all 32 lanes.

        while (events) {

            #if defined (__AVX512F__)
            int bit = __builtin_ctzll(events);
            #elif defined (__AVX2__)
            int bit = __builtin_ctz(events);
            #endif

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
            events &= (events - 1); // clears the lowest bit
        }
        pos += BATCH;
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


