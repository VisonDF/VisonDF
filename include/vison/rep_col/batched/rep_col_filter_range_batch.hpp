#pragma once

template <typename T,
          bool IsBool = false,
          size_t BATCH = 32>
void rep_col_filter_range_batch(std::vector<T>& x,
                                unsigned int& colnb,
                                const std::vector<uint8_t>& mask,
                                const unsigned int& strt_vl)
{
    rep_col_filter_boolmask_batch(x,
                                  colnb,
                                  mask,
                                  strt_vl);
}


