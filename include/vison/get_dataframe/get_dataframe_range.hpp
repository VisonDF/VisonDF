#pragma once

template <AssertionType AssertionLevel = AssertionType::Simple>
void get_dataframe_range(const std::vector<size_t>& cols, 
                         Dataframe& cur_obj,
                         const size_t start,
                         const size_t end)
{

    get_dataframe_range_mt<1,     // CORES
                           false, // NUMA locality
                           AssertionLevel
                            >(cols, 
                              cur_obj,
                              start,
                              end);

}



