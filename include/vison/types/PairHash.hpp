#pragma once

#if defined (__AVX2__)
struct SimdPairHash {
    using sv = std::string_view;
    std::size_t operator()(const std::pair<sv, sv>& p) const noexcept {
        std::size_t h1 = simd_hash{}(p.first);
        std::size_t h2 = simd_hash{}(p.second);
        return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1 << 6) + (h1 >> 2));
    }
};
#endif

struct PairHash {
    using sv = std::string_view;
    std::size_t operator()(const std::pair<sv, sv>& p) const noexcept {
        std::size_t h1 = std::hash<sv>{}(p.first);
        std::size_t h2 = std::hash<sv>{}(p.second);
        return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1 << 6) + (h1 >> 2));
    }
};


