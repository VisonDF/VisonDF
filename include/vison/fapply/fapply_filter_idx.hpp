#pragma once

template <bool IsBool = false,
          bool MapCol = false,
          bool IdxIsTrue = true,
          typename F>
requires FapplyFn<F, first_arg_t<F>>
void fapply_filter_idx(F f, 
                       const unsigned int n, 
                       const std::vector<unsigned int>& mask) 
{

    fapply_filter_idx_mt<IsBool, 
                         MapCol, 
                         1,     // CORES
                         false, // NUMA locality
                         IdxIsTrue
                         >(f,
                           n,
                           mask);

}





