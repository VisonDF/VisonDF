#pragma once

void get_dataframe_filter_range(const std::vector<size_t>& cols, 
                                Dataframe& cur_obj,
                                const std::vector<uint8_t>& mask,
                                const size_t strt_vl)
{

    const size_t n_el = mask.size();

    std::vector<size_t> active_rows;
    active_rows.reserve(n_el);
    const std::vector<std::string>& name_v_row2 = cur_obj.get_rowname();

    if (name_v_row2.empty()) {
      for (size_t r = 0; r < n_el; ++r)
          if (mask[r]) active_rows.push_back(strt_vl + r);
    } else {
      name_v_row.reserve(n_el);
      for (size_t r = 0; r < n_el; ++r)
          if (mask[r]) {
              active_rows.push_back(strt_vl + r);
              name_v_row.push_back(name_v_row2[strt_vl + r]);
          };
    }
   
    nrow = n_el;

    get_dataframe_any(cols,
                      cur_obj,
                      active_rows,
                      nrow); //copied by value here 

}



