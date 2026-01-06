#pragma once

template <unsigned int CORES = 4,
          bool NUMA = false>
void concat_mt(Dataframe& obj) 
{
    const unsigned int& ncol2 = obj.get_ncol();

    if (ncol != ncol2) {
      std::cerr << "Can't concatenate 2 dataframes with different number of columns\n";
      return;
    };
    
    const std::vector<char> type_refv2 = obj.get_typecol();
    
    if (type_refv != type_refv2) {
      std::cerr << "Can't concatenate 2 dataframes with different column type\n";
      return;
    };
  
    const std::vector<std::vector<std::string>>& str_v2  = obj.get_str_vec() ;
    const std::vector<std::vector<CharT>>&       chr_v2  = obj.get_chr_vec() ;
    const std::vector<std::vector<uint8_t>>&     bool_v2 = obj.get_bool_vec();
    const std::vector<std::vector<IntT>>&        int_v2  = obj.get_int_vec() ;
    const std::vector<std::vector<UIntT>>&       uint_v2 = obj.get_uint_vec();
    const std::vector<std::vector<FloatT>>&      dbl_v2  = obj.get_dbl_vec() ;

    const std::vector<std::vector<std::string>>& tmp_val_refv2 = obj.get_tmp_val_refv();
  
    const unsigned int& nrow2 = obj.get_nrow();
    unsigned int pre_nrow = nrow;
    nrow += nrow2;
    const unsigned int local_nrow = nrow;

    if (in_view) {
        row_view_idx.resize(local_nrow);
        for (size_t i = pre_nrow; i < local_nrow; ++i) {
            row_view_idx.push_back(i);
            row_view_map.emplace(i, i);
        }
    }

    for (auto& el : str_v ) { el.resize(local_nrow) };
    for (auto& el : chr_v ) { el.resize(local_nrow) };
    for (auto& el : bool_v) { el.resize(local_nrow) };
    for (auto& el : int_v ) { el.resize(local_nrow) };
    for (auto& el : uint_v) { el.resize(local_nrow) };
    for (auto& el : dbl_v ) { el.resize(local_nrow) };

    if constexpr (CORES > 1) {

        int numa_nodes = 1;
        if (numa_available() >= 0) 
            numa_nodes = numa_max_node() + 1; 

        #pragma omp parallel for num_threads(CORES)
        {

            const int tid        = omp_get_thread_num();
            const int nthreads   = omp_get_num_threads();
            
            MtStruct cur_struct;

            if constexpr (NUMA) {
                numa_mt(cur_struct,
                        matr_idx[0].size(), 
                        tid, 
                        nthreads, 
                        numa_nodes);
            } else {
                simple_mt(cur_struct,
                          matr_idx[0].size(), 
                          tid, 
                          nthreads);
            }
                
            const unsigned int start = cur_struct.start;
            const unsigned int end   = cur_struct.end;

            for (size_t I = start; I < end; ++I) {

                const size_t val_idx = matr_idx[0][I];

                auto& dst = str_v [I];
                auto& src = str_v2[I];

                size_t i2 = 0;
                for (size_t i = pre_nrow; i < local_nrow; ++i, ++i2)
                    dst[i] = src[i2];

            };
        }

    } else {

        for (size_t I = 0; I < matr_idx[0].size(); ++I) {

            const size_t val_idx = matr_idx[0][I];

            auto& dst = str_v [I];
            auto& src = str_v2[I];

            size_t i2 = 0;
            for (size_t i = pre_nrow; i < local_nrow; ++i, ++i2)
                dst[i] = src[i2];

        };

    }

    auto concat_pod_column = [pre_nrow, nrow2](auto& vec1,
                                               const auto& vec2,
                                               const std::vector<size_t>& col_idx) 
    {
        using T = typename std::remove_reference_t<decltype(vec1)>::value_type;
   
        if constexpr (CORES > 1) {

            int numa_nodes = 1;
            if (numa_available() >= 0) 
                numa_nodes = numa_max_node() + 1; 

            #pragma omp parallel num_threads(CORES)
            {

                const int tid        = omp_get_thread_num();
                const int nthreads   = omp_get_num_threads();
                
                MtStruct cur_struct;

                if constexpr (NUMA) {
                    numa_mt(cur_struct,
                            col_idx.size(), 
                            tid, 
                            nthreads, 
                            numa_nodes);
                } else {
                    simple_mt(cur_struct,
                              col_idx.size(), 
                              tid, 
                              nthreads);
                }
                    
                const unsigned int start = cur_struct.start;
                const unsigned int end   = cur_struct.end;

                for (size_t i = start; i < end; ++i) {
                    const size_t val_idx = col_idx[el];
    
                    T* dst = vec1[i].data() + pre_nrow;
                    T* src = vec2[i].data();
                    std::memcpy(dst,
                                src,
                                nrow2 * sizeof(T));
                }
            }

        } else {

            for (size_t i = 0; i < col_idx.size(); ++i) {
                const size_t val_idx = col_idx[el];
    
                T* dst = vec1[i].data() + pre_nrow;
                T* src = vec2[i].data();
                std::memcpy(dst,
                            src,
                            nrow2 * sizeof(T));
            }

        }

    };

    concat_pod_column(chr_v , chr_v2 ,  matr_idx[1]);
    concat_pod_column(bool_v, bool_v2,  matr_idx[2]);
    concat_pod_column(int_v , int_v2 ,  matr_idx[3]);
    concat_pod_column(uint_v, uint_v2,  matr_idx[4]);
    concat_pod_column(dbl_v , dbl_v2 ,  matr_idx[5]);
    
};




