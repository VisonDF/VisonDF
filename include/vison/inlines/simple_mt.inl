#pragma once

inline void simple_mt(MtStruct& cur_struct,
                      const unsigned int local_nrow,
                      const unsigned int tid,
                      const unsigned int nb_threads
                      ) 
{

    const size_t chunk_size = local_nrow / nb_threads;
    const size_t start      = tid * chunk_size; 
    const size_t end        = (start + chunk_size < local_nrow) ? start + chunk_size : local_nrow;

    cur_struct.start = start;
    cur_struct.end   = end;
    cur_struct.len   = end - start;

}


