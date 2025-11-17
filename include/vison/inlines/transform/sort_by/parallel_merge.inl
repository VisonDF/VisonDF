#pragma once

template <unsigned int CORES = 4>
void parallel_merge(const T* A, size_t nA,
                    const T* B, size_t nB,
                    T* out)
{
    #pragma omp parallel num_threads(CORES)
    {
        int tid = omp_get_thread_num();
        size_t start = (nA + nB) * tid     / CORES;
        size_t end   = (nA + nB) * (tid + 1) / CORES;

        // find boundaries using binary search
        size_t a_start = std::lower_bound(A, A + nA, out[start]) - A;
        size_t b_start = start - a_start;

        size_t a_end   = std::lower_bound(A, A + nA, out[end]) - A;
        size_t b_end   = end - a_end;

        // local merge
        std::merge(A + a_start, A + a_end,
                   B + b_start, B + b_end,
                   out + start);
    }
}
