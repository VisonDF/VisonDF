#pragma once

template<bool Simd = true>
void radix_sort_charbuf(
    const std::vector<uint8_t[df_charbuf_size]>* bufv,
    size_t n,
    size_t* idx)
{

    #if !defined(__AVX2__)
    static_assert(!Simd, 
        "Simd=true requires AVX2, but AVX2 is not available on this CPU/compiler.");
    #endif

    std::vector<size_t> tmp(n);
    size_t   count[256];
    std::vector<uint8_t> cur_keys(n);

    size_t* in  = idx;
    size_t* out = tmp.data();

    auto pass = [&](size_t pos)
    {
        memset(count, 0, sizeof(count));

        if constexpr (Simd) {

            for (size_t i = 0; i < n; ++i)
                cur_keys[i] = (*bufv)[in[i]][pos];

            #if defined(__AVX512F__)

                histogram_pass_u8_avx512_16buckets(cur_keys.data(), n, count);
                
            #elif defined(__AVX2__)

                if (n < 200'000) {
                    histogram_pass_u8_avx2         (cur_keys.data(), n, count);
                } else {
                    histogram_pass_u8_avx2_8buckets(cur_keys.data(), n, count);
                }

            #endif
        } else {
                for (size_t i = 0; i < n; i++) {
                    uint8_t k = (*bufv)[in[i]][pos];
                    count[k]++;
                }
        }

        size_t sum = 0;
        for (size_t i = 0; i < 256; i++) {
            size_t c = count[i];
            count[i] = sum;
            sum += c;
        }

        if constexpr (Simd) {
        #if defined(__AVX512F__) 
            scatter_pass_u8_avx512(cur_keys.data(), 
                                    in, 
                                    n, 
                                    count);
        #endif
        } else {
            for (size_t i = 0; i < n; i++) {
                uint8_t k = (*bufv)[in[i]][pos];
                out[count[k]++] = in[i];
            }
        }

    };

    for (int pos = df_charbuf_size - 1; pos >= 0; pos--) {
        pass(pos);

        if constexpr (!Simd)
            std::swap(in, out);
    }

    if constexpr (!Simd) {
        if (in != idx) {
            std::copy(in, in + n, idx);
        }
    }

}




