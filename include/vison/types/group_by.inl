#pragma once

template <typename T>
struct PairGroupBy {
    std::vector<unsigned int> idx_vec;
    T value;
}
