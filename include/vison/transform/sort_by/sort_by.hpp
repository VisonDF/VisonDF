#pragma once

template <bool ASC = 1, 
          bool Simd = true,
          SortType S = SortType::Radix,
          typename ComparatorFactory = DefaultComparatorFactory>
void sort_by(unsigned int& n) {

    sort_by_mt<ASC, 
               1, 
               Simd, 
               S, 
               ComparatorFactory>(n);
      
};


