
inline void parse_rows_chunk(
    std::string_view csv_view,
    size_t start_byte,
    size_t end_byte,
    std::vector<std::vector<std::string_view>>& columns,
    char delim,
    char str_context,
    unsigned int ncol
) noexcept
{
    const char* base = csv_view.data();
    const size_t N = csv_view.size();
    if (start_byte >= N) return;
    end_byte = std::min(end_byte, N);

    size_t pos = start_byte;
    bool in_quotes = false;
    size_t field_start = start_byte;
    size_t verif_ncol = 0;

    static const __m256i NL = _mm256_set1_epi8('\n');
    static const __m256i CR = _mm256_set1_epi8('\r');
    __m256i D = _mm256_set1_epi8(delim);
    __m256i Q = _mm256_set1_epi8(str_context);

    for (; pos + 32 <= N; ) {
        _mm_prefetch(base + pos + 512, _MM_HINT_T0);
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(base + pos));

        int mD  = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, D));
        int mQ  = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, Q));
        int mNL = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, NL));
        int mCR = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, CR));

        int mNL_any = (mNL | mCR);
        int32_t events  = (mD | mNL_any | mQ);

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
            int bit = __builtin_ctz(events);
            size_t idx = pos + bit;
            char c = base[idx];

            if (c == str_context) {
                in_quotes = !in_quotes;
            }
            else if (!in_quotes && c == delim) {
                std::string_view field = csv_view.substr(field_start, idx - field_start);
                columns[verif_ncol].emplace_back(
                    field.empty() ? std::string_view("NA") : field
                );


                ++verif_ncol;
                field_start = idx + 1;
            }
            else if (!in_quotes && (c == '\n' || c == '\r')) {
                std::string_view field = csv_view.substr(field_start, idx - field_start);
                columns[verif_ncol].emplace_back(
                    field.empty() ? std::string_view("NA") : field
                );
 
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

        pos += 32;
        next_chunk:
          continue;
    }

    for (; pos < N; ++pos) {
        char c = base[pos];
        if (c == str_context) {
            in_quotes = !in_quotes;
        } else if (!in_quotes && c == delim) {
            std::string_view field = csv_view.substr(field_start, pos - field_start);
            columns[verif_ncol].emplace_back(
                field.empty() ? std::string_view("NA") : field
            );

            ++verif_ncol;
            field_start = pos + 1;
        } else if (!in_quotes && (c == '\n' || c == '\r')) {
            std::string_view field = csv_view.substr(field_start, pos - field_start);
            columns[verif_ncol].emplace_back(
                field.empty() ? std::string_view("NA") : field
            );

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
        std::string_view field = csv_view.substr(field_start, N - field_start);
        columns[verif_ncol].emplace_back(
            field.empty() ? std::string_view("NA") : field
        );
    }

}


