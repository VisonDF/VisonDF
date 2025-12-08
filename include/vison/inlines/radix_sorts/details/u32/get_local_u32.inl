#pragma once

inline std::vector<size_t>& get_local_count_u32() {
    static thread_local std::vector<size_t> local(RADIX_KI32);
    return local;
}

inline std::vector<size_t>& get_local_bucket_size_u32() {
    static thread_local std::vector<size_t> local(RADIX_KI32);
    return local;
}

inline std::vector<size_t>& get_local_bucket_base_u32() {
    static thread_local std::vector<size_t> local(RADIX_KI32);
    return local;
}

inline std::vector<std::vector<size_t>>& get_local_thread_off_u32() {
    const int mx_threads = omp_get_max_threads();
    static thread_local std::vector<std::vector<size_t>> local(mx_threads, 
                                                         std::vector<size_t>(RADIX_KI32));
    return local;
}

inline std::vector<size_t>& get_local_hist_u32() {
    const int mx_threads = omp_get_max_threads();
    static thread_local std::vector<std::vector<size_t>> local(mx_threads, 
                                                         std::vector<size_t>(RADIX_KI32));
    return local;
}




