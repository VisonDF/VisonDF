#pragma once

template <typename F, typename T>
concept FapplyFn =
    std::is_invocable_r_v<void, F, T&>;

template <typename F>
struct first_arg;

// matches free funtions
template <typename R, typename T>
struct first_arg<R(*)(T&)> {
    using type = T;
};

//matches lambdas
template <typename R, typename C, typename T>
struct first_arg<R(C::*)(T&) const> {
    using type = T;
};

template <typename F>
using first_arg_t = typename first_arg<F>::type;
