#pragma once

template <typename T>
struct ReservingVec {
    std::vector<T> v;
    ReservingVec(std::size_t n) {
        v.reserve(n);
    }
};
