#pragma once

inline void numa_mt(
                     MtStruct& cur_struct,
                     const unsigned int local_nrow,
                     const unsigned int tid,
                     const unsigned int nb_threads,
                     const unsigned int numa_nodes
                   )
{

    const unsigned int threads_per_node = nb_threads / numa_nodes;

    assert(numa_nodes > 0);
    assert(threads_per_node > 0);
    assert(nb_threads == threads_per_node * numa_nodes);
    assert(tid < nb_threads);

    const int node_id    = tid / threads_per_node;
    const int local_tid  = tid % threads_per_node;
    
    const size_t node_chunk = local_nrow / numa_nodes;
    const size_t node_start = node_id * node_chunk;
    const size_t node_end =
        (node_id == numa_nodes - 1) ? local_nrow : node_start + node_chunk;
    
    const size_t local_chunk =
        (node_end - node_start) / threads_per_node;
    
    const size_t start =
        node_start + local_tid * local_chunk;
    const size_t end =
        (local_tid == threads_per_node - 1)
            ? node_end
            : start + local_chunk;

    cur_struct.start = start;
    cur_struct.end   = end;
    cur_struct.len   = end - start;
}



