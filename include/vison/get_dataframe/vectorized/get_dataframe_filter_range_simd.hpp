#pragma once

void get_dataframe_filter_range_simd(const std::vector<size_t>& cols, 
                                     Dataframe& cur_obj,
                                     const std::vector<uint8_t>& mask,
                                     const size_t strt_vl)
{

    const size_t tot_nrow = cur_obj.get_nrow();

    std::vector<size_t> active_rows;
    active_rows.reserve(tot_nrow);
    const std::vector<std::string>& name_v_row2 = cur_obj.get_rowname();

    if (name_v_row2.empty()) {
      for (size_t r = strt_vl; r < strt_vl + tot_nrow; ++r)
          if (mask[r]) active_rows.push_back(r);
    } else {
      name_v_row.reserve(tot_nrow);
      for (size_t r = strt_vl; r < strt_vl + tot_nrow; ++r)
          if (mask[r]) {
              active_rows.push_back(r);
              name_v_row.push_back(name_v_row2[r]);
          };
    }
   
    nrow = active_rows.size();

    get_dataframe_filter_any_simd(cols,
                                  cur_obj,
                                  active_rows,
                                  nrow); //copied by value here

}



