#pragma once

void get_dataframe(const std::vector<size_t>& cols, 
                   Dataframe& cur_obj)
{

    get_dataframe_range_mt<1,    // CORES
                           false // NUMA locality
                            >(cols, 
                              cur_obj,
                              0, // start
                              cur_obj.get_nrow());
}



