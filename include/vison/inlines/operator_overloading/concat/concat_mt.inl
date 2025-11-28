#pragma once

template <unsigned int CORES, typename DF>
struct ConcatMt {
    const DF* other;

    DF& operator()(DF& df) const {
        df.template concat_mt<CORES>(*other);
        return df;
    }
};

template <unsigned int CORES = 4, typename DF>
inline ConcatMt<CORES, DF> concat_mt(const DF& other) {
    return ConcatMt<CORES, DF>{ &other };
}

template <typename DF, unsigned int CORES>
inline DF& operator|(DF& df, const ConcatMt<CORES, DF>& c) {
    return c(df);
}
