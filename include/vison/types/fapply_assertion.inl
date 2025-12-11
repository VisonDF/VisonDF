#pragma once

template <typename F, typename T>
concept FapplyFn =
    std::is_invocable_r_v<void, F, T&>;

template <typename F>
struct first_arg : first_arg<decltype(&F::operator())> {};

// matches free functions
template <typename R, typename T>
struct first_arg<R(*)(T&)> {
    using type = T;
};

// matches lambdas
template <typename R, typename C, typename T>
struct first_arg<R(C::*)(T&) const> {
    using type = T;
};

template <typename F>
using first_arg_t = typename first_arg<F>::type;

struct DefaultFn {
    template <typename T>
    void operator()(T& x) const noexcept {
      if constexpr (std::is_same_v<T, std::string>) {
          auto is_space = [](char c) noexcept {
              return c == ' ' || c == '\t' || c == '\n' ||
                     c == '\r' || c == '\f' || c == '\v';
          };

          char* begin = x.data();
          char* end   = begin + x.size();

          while (begin < end && is_space(*begin))
              ++begin;

          while (end > begin && is_space(*(end - 1)))
              --end;

          x.assign(begin, end);
      }
    }
};



