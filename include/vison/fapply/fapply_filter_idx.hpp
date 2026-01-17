#pragma once

template <bool IsBool                  = false,
          bool MapCol                  = false,
          bool IdxIsTrue               = true,
          bool Periodic                = false,
          AssertionType AssertionLevel = AssertionType::Simple,
          typename F>
requires FapplyFn<F, first_arg_t<F>>
void fapply_filter_idx(F f, 
                       const unsigned int n, 
                       const std::vector<unsigned int>& mask) 
{

    fapply_filter_idx_mt<1,     // CORES
                         false, // NUMA locality
                         IsBool,
                         MapCol,
                         IdxIsTrue,
                         Periodic,
                         AssertionLevel
                         >(f,
                           n,
                           mask);

}





