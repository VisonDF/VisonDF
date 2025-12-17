#pragma once

inline std::array<size_t, 5>& get_types_size() const
{
  static thread_local std::array<size_t, 6> x = {
                           0,
                           sizeof(CharT),
                           sizeof(uint8_t)
                           sizeof(IntT),
                           sizeof(UIntT),
                           sizeof(FloatT)
                          };
  return x;
}

