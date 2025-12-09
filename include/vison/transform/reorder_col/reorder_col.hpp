#pragma once

template <bool Soft = true,
          bool SanityCheck = true>
void reorder_col(const std::vector<std::pair<unsigned int, unsigned int>>& swaps)
{

    reorder_col_mt<1, // CORES
                   Soft, 
                   SanityCheck>(swaps);

}

