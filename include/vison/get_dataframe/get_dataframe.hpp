#pragma once

template <AssertionType AssertionLevel = AssertionType::Simple>
void get_dataframe(const std::vector<size_t>& cols, 
                   Dataframe& cur_obj)
{

    get_dataframe_range_mt<1,     // CORES
                           false, // NUMA locality
                           MtMethod::Col,
                           AssertionLevel
                          >(
          cols, 
          cur_obj,
          0,                 // start
          cur_obj.get_nrow() // end
    );
}



