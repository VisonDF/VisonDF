#pragma once

template <typename Set, typename T>
bool contains_all(const Set& set, const std::vector<T>& range) {
    for (const auto& x : range) {
        if (!set.contains(x)) {
            return false;
        }
    }
    return true;
}
