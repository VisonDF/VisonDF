#pragma once

template <typename T>
struct PairGroupBy {
    ReservingVec<unsigned int> idx_vec;
    element_type_t<T> value;

    explicit PairGroupBy(std::size_t n)
        : idx_vec(n), value{} {}
};

template <typename U>
struct PairGroupBy<ReservingVec<U>> {
    ReservingVec<unsigned int> idx_vec;
    ReservingVec<U> value;

    explicit PairGroupBy(std::size_t n)
        : idx_vec(n), value(n) {}
};

