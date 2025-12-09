#pragma once

template <typename T>
inline void apply_permutation(std::vector<T>& v, 
                              std::vector<size_t>& perm
                              ) 
{
    const size_t n = v.size();

    for (size_t i = 0; i < n; ++i) {
        size_t current = i;
        while (perm[current] != current) {
            size_t next = perm[current];
            std::swap(v[current], v[next]);
            std::swap(perm[current], perm[next]);
        }
    }
}
