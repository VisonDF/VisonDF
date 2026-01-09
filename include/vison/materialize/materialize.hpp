#pragma once

template <bool IsDense = true,
          bool MemClean = false>
void materialize() 
{

    materialize_mt<1, 
                   IsDense, 
                   MemClean>();

}


