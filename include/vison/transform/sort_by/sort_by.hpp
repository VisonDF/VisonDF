#pragma once

template <bool ASC = 1, 
          bool Simd = true,
          bool Soft = true,
          SortType S = SortType::Radix,
          typename ComparatorFactory = DefaultComparatorFactory>
void sort_by(unsigned int& n) {

    sort_by_mt<ASC, 
               1, 
               Simd,
               Soft,
               S, 
               ComparatorFactory>(n);
      
};


