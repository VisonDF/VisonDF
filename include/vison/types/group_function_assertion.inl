#pragma once

template <typename F>
struct first_arg_grp;  

// Free function pointer
template <typename R, typename T>
struct first_arg_grp<R(*)(std::vector<T>&)> {
    using type = T;
};

template <typename F>
using first_arg_grp_t = typename first_arg_grp<F>::type;

template <typename F, typename T>
concept GroupFn =
requires (F f, std::vector<T>& v) {
    { f(v) } -> std::same_as<T>;
};

int default_groupfn_impl(std::vector<int>& v)
{
    return v.empty() ? 0 : v[0];
}



