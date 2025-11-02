#pragma once

void display_filter_idx(std::vector<int> &x, std::vector<int> &colv) {
  longest_determine();
  unsigned int i2b;
  unsigned int i3;
  unsigned int i4;

  if (colv[0] == -1) {
    colv.resize(ncol);
    for (i2b = 0; i2b < ncol; i2b += 1) {
      colv[i2b] = i2b;
    };
  };

  if (x[0] == -1) {
    x.resize(ncol);
    for (i2b = 0; i2b < nrow; i2b += 1) {
      x[i2b] = i2b;
    };
  };

  unsigned int max_nblngth = 0;
  if (name_v_row.size() == 0) {
    max_nblngth = std::to_string(nrow).length();
  } else {
    for (auto& el : name_v_row) {
      if (el.size() > max_nblngth) {
        max_nblngth = el.size();
      };
    };
  };

  for (i2b = 0; i2b < max_nblngth + 2; ++i2b) {
    std::cout << " ";
  };

  std::string cur_str;
  for (auto& i2 : colv) {
    if (type_refv[i2] == 's') {
      cur_str = "<str>";
      if (longest_v[i2] < 5) {
        longest_v[i2] = 5;
      };
    } else if (type_refv[i2] == 'c') {
      cur_str = "<char>";
      if (longest_v[i2] < 6) {
        longest_v[i2] = 6;
      };
    } else if (type_refv[i2] == 'b') {
      cur_str = "<bool>";
      if (longest_v[i2] < 6) {
        longest_v[i2] = 6;
      };
    } else if (type_refv[i2] == 'i') {
      cur_str = "<int>";
      if (longest_v[i2] < 5) {
        longest_v[i2] = 5;
      };
    } else if (type_refv[i2] == 'u') {
      cur_str = "<uint>";
      if (longest_v[i2] < 6) {
        longest_v[i2] = 6;
      };
    } else if (type_refv[i2] == 'd') {
      cur_str = "<double>";
      if (longest_v[i2] < 8) {
        longest_v[i2] = 8;
      };
    };
    std::cout << cur_str << " ";  


    for (i4 = cur_str.length(); i4 < longest_v[i2]; ++i4) {
      std::cout << " ";
    }; 

  }; 

  std::cout << "\n";
  for (i2b = 0; i2b < max_nblngth + 2; ++i2b) {
    std::cout << " ";
  };
  if (name_v.size() > 0) {
    for (auto& i2 : colv) {
      cur_str = name_v[i2];
      std::cout << cur_str << " ";  
      for (i4 = cur_str.length(); i4 < longest_v[i2]; ++i4) {
        std::cout << " ";
      };

    };
  } else {
    for (auto& i2 : colv) {
      cur_str = "[" + std::to_string(i2) + "]";
      std::cout << cur_str << " ";
      for (i4 = cur_str.length(); i4 < longest_v[i2]; ++i4) {
        std::cout << " ";
      };
    };
  };
  std::cout << "\n";
  if (name_v_row.size() == 0) {
    i4 = 0;
    for (const auto& el : x) {
      std::cout << ":" << i4 << ": ";
      for (i3 = std::to_string(i4).length(); i3 < max_nblngth; ++i3) {
        std::cout << " ";
      };
      for (auto& i2 : colv) {
        cur_str = tmp_val_refv[i2][el]; 
        std::cout << cur_str << " "; 
        for (i3 = cur_str.length(); i3 < longest_v[i2]; ++i3) {
          std::cout << " ";
        };
      };
      std::cout << "\n";
      i4 += 1;

    };
  } else {
    for (const auto& el : x) {
      std::cout << std::setw(max_nblngth) << name_v_row[el] << " : ";
      for (auto& i2 : colv) {
        cur_str = tmp_val_refv[i2][el];
        std::cout << cur_str << " ";
        for (i3 = cur_str.length(); i3 < longest_v[i2]; ++i3) {
          std::cout << " ";
        };
      };
      std::cout << "\n";
    };
  };
};


