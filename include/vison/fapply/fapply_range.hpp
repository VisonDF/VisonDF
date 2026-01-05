#pragma once

template <bool IsBool = false,
          bool MapCol = false,
          typename F>
requires FapplyFn<F, first_arg_t<F>>
void fapply_range(F f, 
                  const unsigned int n,
                  const unsigned int strt,
                  const unsigned int end) 
{

    fapply_range_mt<IsBol, MapCol, 1>(f,
                                      n,
                                      strt,
                                      end);

}



