#pragma once

template <bool NUMA = false>
void concat(Dataframe& obj) 
{

    concat_mt<1, // CORES
              NUMA>(obj);

};




