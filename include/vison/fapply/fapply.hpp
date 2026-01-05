#pragma once

template <bool IsBool = false,
          bool MapCol = false,
          typename F>
requires FapplyFn<F, first_arg_t<F>>
void fapply(F f, 
            const unsigned int& n) {

   const unsigned int local_nrow = nrow;
   fapply_range_mt<IsBool, MapCol, 1>(f, 
                                       n, 
                                       0, // strt
                                       local_nrow);

}



