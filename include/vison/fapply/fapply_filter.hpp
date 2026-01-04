#pragma once

template <bool IsBool = false,
          bool MapCol = false,
          typename F>
requires FapplyFn<F, first_arg_t<F>>
void fapply_filter(F f, 
                   unsigned int n, 
                   const std::vector<uint8_t>& mask) 
{

    fapply_filter_range<IsBool, MapCol>(f,
                                        n,
                                        mask,
                                        0);
}



