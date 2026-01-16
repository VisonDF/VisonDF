#pragma once

template <bool IsBool                  = false,
          bool MapCol                  = false,
          AssertionType AssertionLevel = AssertionType::None,
          typename T> 
void rep_col(std::vector<T> &x, unsigned int &colnb) {

    rep_col_mt<1,     // CORES
               false, // NUMA locality
                IsBool,
                MapCol,
                AssertionLevel
               >(x, colnb);

};




