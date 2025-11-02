inline bool has_dot(const std::string_view& s) noexcept {
    const char* p = s.data();
    const char* end = p + s.size();
    for (; p + 8 <= end; p += 8) {
        uint64_t chunk;
        std::memcpy(&chunk, p, 8);
        if ((((chunk ^ 0x2E2E2E2E2E2E2E2EULL) - 0x0101010101010101ULL) & ~chunk & 0x8080808080808080ULL) != 0)
            return true;
    }
    for (; p < end; ++p)
        if (*p == '.') return true;
    return false;
}
