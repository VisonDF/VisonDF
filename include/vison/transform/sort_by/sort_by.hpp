#pragma once

template <bool ASC = 1, 
          typename T = void,
          bool Simd = true,
          bool Soft = true,
          SortType S = SortType::Radix,
          typename ComparatorFactory = DefaultComparatorFactory>
void sort_by(unsigned int& n) {

    sort_by_mt<ASC, 
               1,
               T,
               Simd,
               Soft,
               S, 
               ComparatorFactory>(n);
      
};


