#pragma once

template <typename DF>
struct Concat {
    const DF* other;

    DF& operator()(DF& df) const {
        df.concat(*other);
        return df;
    }
};

template <typename DF>
inline Concat<DF> concat(const DF& other) {
    return Concat{ &other };
}

template<typename DF>
inline DF& operator|(DF& df, const Concat<DF>& c) {
    return c(df);
}


