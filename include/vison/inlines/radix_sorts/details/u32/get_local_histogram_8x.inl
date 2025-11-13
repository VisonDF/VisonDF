#pragma once

inline size_t* get_local_histogram_8x() {
    static thread_local std::array<size_t, RADIX_LANES * RADIX_KI32> local{};
    return local.data();
}
