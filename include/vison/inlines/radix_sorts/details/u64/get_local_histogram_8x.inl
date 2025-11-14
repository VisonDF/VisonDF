#pragma once

inline size_t* get_local_histogram_8x_u64() {
    static thread_local std::array<size_t, RADIX_LANES * RADIX_KI64> local{};
    return local.data();
}
