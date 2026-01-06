#pragma once

template <bool NUMA = false,
          bool BoolAsU8 = false,
          typename T> 
void add_col(const std::vector<T> &x, 
             const std::string name = "NA") 
{

    add_col_mt<1, // CORES
               NUMA,
               BoolAsU8>(x, name);

};



