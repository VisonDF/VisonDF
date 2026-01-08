#pragma once

template <unsigned int CORES = 4,
          bool NUMA = false
         >
void get_dataframe_mt(const std::vector<size_t>& cols, 
                      Dataframe& cur_obj)
{

    get_dataframe_range_mt<CORES, 
                           NUMA>(cols, 
                                 cur_obj,
                                 0, // start
                                 cur_obj.get_nrow());

}



