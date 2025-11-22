#pragma once

template <bool ASC, 
          unsigned int CORES = 4,
          bool Simd = true,
          SortType S = SortType::Radix,
          bool Flat = true,
          char PaddingChar = 0x00,
          typename ComparatorFactory = DefaultComparatorFactory>
inline void sort_string(
    std::vector<size_t>& idx, 
    const std::string* col,
    ComparatorFactory make_cmp = ComparatorFactory{})
{

    auto cmp = make_cmp.template operator()<ASC, std::string>(col);
    static_assert(IndexComparator<decltype(cmp)>,
              "Comparator must be cmp(size_t,size_t)->bool");

    if constexpr (S == SortType::Radix) {

            if constexpr (Flat) {

                // TODO when ghost mode is activated, just have to check 
                // if modified, then compute if not, just get the old 
                // max_length for this string col
                const size_t max_length = max_chars_string_col<CORES, Simd>(col);
                std::vector<uint8_t> tkeys(nrow * max_length);
                const uint8_t pad = uint8_t(PaddingChar) ^ 0x80u;

                for (size_t i = 0; i < nrow; ++i) {
                    const std::string& s = col[i];
                    const size_t len = s.size();

                    uint8_t* dst = tkeys.data() + i * max_length;
                
                    for (size_t j = 0; j < len; ++j)
                        dst[j] = uint8_t(s[j]) ^ 0x80u;
                
                    std::memset(dst + len, pad, max_length - len);
                }

                sort_char_from_string<ASC, 
                                      CORES, 
                                      Simd>(idx.data(), tkeys.data(), nrow);

            } else if constexpr (!Flat) {

                if constexpr (CORES > 1) {

                } else if constexpr (CORES <= 1) {

                }

            }

            return;

    } else if constexpr (S == SortType::Standard) {

            if constexpr (CORES == 1) {
    
            std::stable_sort(idx.begin(), idx.end(), cmp);

            } else if constexpr (CORES > 1) {
 
                std::vector<std::pair<size_t, size_t>> chunks(CORES);
               
                // precomputation of the chunks
                for (size_t t = 0; t < CORES; t++) {
                    size_t start = nrow * t / CORES;
                    size_t end   = nrow * (t + 1) / CORES;
                    chunks[t] = {start, end};
                }
                
                // parallel sorting
                #pragma omp parallel num_threads(CORES)
                {
                    int tid = omp_get_thread_num();
                    auto [start, end] = chunks[tid];
                    std::sort(idx.begin() + start, idx.begin() + end, cmp);
                }

                std::vector<size_t> tmp(nrow);
                bool flip = false;
                
                while (chunks.size() > 1) {
                
                    std::vector<std::pair<size_t,size_t>> next;
                    next.resize(chunks.size() / 2);   
                
                    const size_t end_loop = chunks.size() - 1;

                    #pragma omp parallel for num_threads(CORES)
                    for (size_t i = 0; i < end_loop; i += 2) {
                        auto [s1, e1] = chunks[i];
                        auto [s2, e2] = chunks[i + 1];

                        size_t* A1;
                        size_t* A2;
                        size_t* B1;
                        size_t* B2;
                        size_t* Out;
                        
                        if (!flip) {
                            A1 = idx.data() + s1;
                            A2 = A1 + (e1 - s1);
                        
                            B1 = idx.data() + e1;
                            B2 = B1 + (e2 - e1);
                        
                            Out = tmp.data() + s1;
                        }
                        else {
                            A1 = tmp.data() + s1;
                            A2 = A1 + (e1 - s1);
                        
                            B1 = tmp.data() + e1;
                            B2 = B1 + (e2 - e1);
                        
                            Out = idx.data() + s1;
                        }
                        
                        std::merge(A1, A2, B1, B2, Out, cmp);
                
                        next[i / 2] = {s1, e2};
                    }
                
                    if (chunks.size() % 2 == 1) {
                        next.push_back(chunks.back());
                    }
                
                    chunks = std::move(next);
                    flip = !flip;
                }
                
                if (flip)
                    idx = tmp;

            }

    }

    if constexpr (!ASC) {
        std::reverse(idx.begin(), idx.end());
    }

}



