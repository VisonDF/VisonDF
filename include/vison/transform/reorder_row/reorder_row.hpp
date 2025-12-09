#pragma once

template <bool Soft = true,
          bool SanityCheck = true>
void reorder_row(std::vector<std::pair<unsigned int, unsigned int>>& swaps) 
{
    reorder_row_mt<1          //CORES
                   Soft,
                   SanityCheck>(swaps);
}

