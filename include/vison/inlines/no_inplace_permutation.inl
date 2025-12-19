#pragma once

template <unsigned int CORES = 4,
          typename T>
inline void no_inplace_permutation(std::vector<std::vector<T>>& v,
                                   const std::vector<unsigned int>& perm)
{
    const size_t local_nrow = v[0].size();
    const size_t local_ncol = v.size();
    std::vector<T> tmp(local_nrow);

    #pragma omp parallel for if (CORES > 1) num_threads(CORES) firstprivate(tmp)
    for (size_t col = 0; col < local_ncol; ++col) {

	if (std::find(col_alrd_materialized.begin(), 
	              col_alrd_materialized.end(), col) != col_alrd_materialized.end())
		continue;

        auto& vc = v[col];
        size_t i = 0;

        if constexpr (sizeof(T) == 4) {
            #if defined(__AVX512F__)
            for (; i + 16 <= nrow; i += 16) {
                __m512i index_vec = _mm512_loadu_si512((const void*)&perm[i]);
                if constexpr (std::is_same_v<T, float>) {
                    __m512 gathered = _mm512_i32gather_ps(index_vec, vc.data(), sizeof(T));
                    _mm512_storeu_ps(&tmp[i], gathered);
                } else {
                    __m512i gathered = _mm512_i32gather_epi32(index_vec, vc.data(), sizeof(T));
                    _mm512_storeu_si512(&tmp[i], gathered);
                }
            }
            #elif defined(__AVX2__)
            for (; i + 8 <= nrow; i += 8) {
                __m256i index_vec = _mm256_loadu_si256((const __m256i*)&perm[i]);
                if constexpr (std::is_same_v<T, float>) {
                    __m256 gathered = _mm256_i32gather_ps(vc.data(), index_vec, sizeof(T));
                    _mm256_storeu_ps(&tmp[i], gathered);
                } else {
                    __m256i gathered = _mm256_i32gather_epi32(vc.data(), index_vec, sizeof(T));
                    _mm256_storeu_si256(&tmp[i], gathered);
                }
            }
            #endif
        } else if constexpr (sizeof(T) == 8) {
            #if defined(__AVX512F__)
            for (; i + 8 <= nrow; i += 8) {
                __m256i index_vec = _mm256_loadu_si256((const void*)&perm[i]);
                if constexpr (std::is_same_v<T, double>) {
                    __m512d gathered = _mm512_i32gather_pd(index_vec, vc.data(), sizeof(T));
                    _mm512_storeu_pd(&tmp[i], gathered);
                } else {
                    __m512i gathered = _mm512_i32gather_epi64(index_vec, vc.data(), sizeof(T));
                    _mm512_storeu_si512(&tmp[i], gathered);
                }
            }
        
            #elif defined(__AVX2__)
            for (; i + 4 <= nrow; i += 4) {
                __m128i index_vec = _mm_loadu_si128((const __m128i*)&perm[i]);
        
                if constexpr (std::is_same_v<T, double>) {
                    __m256d gathered = _mm256_i32gather_pd(vc.data(), index_vec, sizeof(T));
                    _mm256_storeu_pd(&tmp[i], gathered);
                } else {
                    __m256i gathered = _mm256_i32gather_epi64(vc.data(), index_vec, sizeof(T));
                    _mm256_storeu_si256(&tmp[i], gathered);
                }
            }
            #endif
        }

        for (; i < nrow; ++i)
            tmp[i] = vc[perm[i]];

        for (size_t r = 0; r < nrow; ++r)
            vc[r] = tmp[r];
    }
    col_alrd_materialized.clear();
}




