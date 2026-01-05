#pragma once

template <bool IsBool = false,
          bool MapCol = false,
          unsigned int CORES = 4,
          typename F>
requires FapplyFn<F, first_arg_t<F>>
void fapply_mt(F f, 
               const unsigned int& n) {

   const unsigned int local_nrow = nrow;
   fapply_range_mt<IsBool, MapCol, CORES>(f, 
                                          n, 
                                          0, // strt
                                          local_nrow);

}



