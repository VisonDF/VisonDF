#pragma once

void dislay_range(
                  std::vector<unsigned int>& cols,
                  const unsigned int strt,
                  const unsigned int end
                  )
{

    dislay_range_mt<1>(cols, strt, end);

}



