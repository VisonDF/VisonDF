#pragma once

inline std::vector<size_t>& get_local_count_u16() {
    static thread_local std::array<size_t> local(RADIX_KI16);
    return local.data();
}

inline std::vector<size_t>& get_local_bucket_base_u16() {
    static thread_local std::array<size_t> local(RADIX_KI16);
    return local.data();
}

inline std::vector<size_t>& get_local_bucket_size_u16() {
    static thread_local std::array<size_t> local(RADIX_KI16);
    return local.data();
}

inline std::vector<std::vector<size_t>>& get_local_hist_u16() {
    const int mx_threads = omp_get_max_threads();
    static thread_local std::vector<std::vector<size_t>> local(mx_threads, 
                                                         std::vector<size_t>(RADIX_KI16));
    return local.data();
}

inline std::vector<std::vector<size_t>>& get_local_thread_off_u16() {
    const int mx_threads = omp_get_max_threads();
    static thread_local std::vector<std::vector<size_t>> local(mx_threads, 
                                                         std::vector<size_t>(RADIX_KI16));
    return local.data();
}

inline size_t* get_local_histogram_8x_u16()
{
    static thread_local std::vector<size_t> local_hist(RADIX_LANES * RADIX_KI16);
    return local_hist.data();
}

inline size_t* get_local_histogram_16x_u16()
{
    static thread_local std::vector<size_t> local_hist(RADIX_LANES_AVX512 * RADIX_KI16);
    return local_hist.data();
}


