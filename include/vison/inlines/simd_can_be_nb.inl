inline bool simd_can_be_nb(const char* s, size_t len) noexcept {
    if (len == 0) return false;

    static const __m256i zero = _mm256_set1_epi8('0');
    static const __m256i nine = _mm256_set1_epi8('9');
    static const __m256i dot  = _mm256_set1_epi8('.');

    bool dot_seen = false;
    size_t i = 0;

    for (; i + 32 <= len; i += 32) {
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(s + i));

        __m256i ge0 = _mm256_cmpgt_epi8(chunk, _mm256_sub_epi8(zero, _mm256_set1_epi8(1))); // >= '0'
        __m256i le9 = _mm256_cmpgt_epi8(_mm256_add_epi8(nine, _mm256_set1_epi8(1)), chunk); // <= '9'
        __m256i is_digit = _mm256_and_si256(ge0, le9);
        __m256i is_dot   = _mm256_cmpeq_epi8(chunk, dot);
        __m256i allowed  = _mm256_or_si256(is_digit, is_dot);

        int allowed_mask = _mm256_movemask_epi8(allowed);
        if (allowed_mask != 0xFFFFFFFF) return false;

        int dot_mask = _mm256_movemask_epi8(is_dot);
        int dot_count = __builtin_popcount(dot_mask);
        if (dot_count > 0) {
            if (dot_seen || dot_count > 1)
                return false;
            dot_seen = true;
        }
    }

    for (; i < len; ++i) {
        unsigned char c = s[i];
        if (c == '.') {
            if (dot_seen) return false;
            dot_seen = true;
        } else if (c < '0' || c > '9') {
            return false;
        }
    }

    return true;
}
