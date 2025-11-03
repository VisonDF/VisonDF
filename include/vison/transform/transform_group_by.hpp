#pragma once

void transform_group_by(std::vector<unsigned int> &x,
                        std::string sumcolname = "n") {
  //std::unordered_map<std::string, unsigned int> lookup; // standard map (slower)
  ankerl::unordered_dense::map<std::string, unsigned int> lookup;
  std::vector<unsigned int> occ_v;
  std::vector<std::string> occ_v_str;
  occ_v_str.reserve(nrow);
  occ_v.reserve(nrow);
  std::string key;
  std::vector<std::string> key_vec(nrow);
  unsigned int i;
  unsigned int i2;
  for (i = 0; i < nrow; i += 1) {
    key = tmp_val_refv[x[0]][i];
    for (i2 = 1; i2 < x.size(); i2 += 1) {
      key += tmp_val_refv[x[i2]][i];
    };
    lookup[key] += 1;
    key_vec[i] = key;
  };
  unsigned int occ_val;
  for (auto& el : key_vec) {
    occ_val = lookup[el];
    occ_v.push_back(occ_val);
    occ_v_str.push_back(std::to_string(occ_val));
  };
  uint_v.insert(uint_v.end(), occ_v.begin(), occ_v.end());
  tmp_val_refv.push_back(occ_v_str);
  if (name_v.size() > 0) {
    name_v.push_back(sumcolname);
  };
  type_refv.push_back('u');
  ncol += 1;
};


