struct SimdCountLines {
    size_t count;
    std::vector<size_t> positions;
};

inline SimdCountLines simd_count_newlines(const char* data, size_t size) noexcept {
    SimdCountLines result;
    result.positions.reserve(size / 64); 

    const __m256i NL = _mm256_set1_epi8('\n');

    size_t pos = 0;
    size_t count = 0;

    for (; pos + 32 <= size; pos += 32) {
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + pos));

        int mNL = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, NL));

        while (mNL) {
            int bit = __builtin_ctz(mNL);
            result.positions.push_back(pos + bit);
            mNL &= (mNL - 1);
            ++count;
        }

        _mm_prefetch(data + pos + 512, _MM_HINT_T0);
    }

    for (; pos < size; ++pos) {
        if (data[pos] == '\n') {
            result.positions.push_back(pos);
            ++count;
        }
    }

    result.count = count;
    return result;
}
