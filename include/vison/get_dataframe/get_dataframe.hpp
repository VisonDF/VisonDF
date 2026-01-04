#pragma once

void get_dataframe(const std::vector<size_t>& cols, 
                   Dataframe& cur_obj)
{
    get_dataframe_mt<1>(cols, cur_obj);
}



