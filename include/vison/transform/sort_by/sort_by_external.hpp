#pragma once

template <typename T,
          bool ASC = true,
          bool Simd = true,
          SortType S = SortType::Radix,
          bool BoolAsU8 = true,
          bool IsBoolCompressed = false>
void sort_by_external(const std::vector<T>& nvec) {

    sort_by_external<T,
                     ASC,
                     1
                     Simd,
                     S,
                     BoolAsU8,
                     IsBoolCompressed>(nvec);
      
};


