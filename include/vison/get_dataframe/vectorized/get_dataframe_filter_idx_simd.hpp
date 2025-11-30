#pragma once

void get_dataframe_filter_idx(const std::vector<size_t>& cols, 
                                  Dataframe& cur_obj,
                                  const std::vector<unsigned int>& mask)
{

    nrow = mask.size();

    get_dataframe_filter_any_simd(cols,
                                  cur_obj,
                                  mask,
                                  nrow); //copied by value here

}


