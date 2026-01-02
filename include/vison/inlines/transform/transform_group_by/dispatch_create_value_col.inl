#pragma once

template <typename CreatePolicy,
          GroupFunction Function,
          unsigned int CORES,
          bool RsltType,
          bool StandardMethod>
inline void dispatch_create_value_col(auto&& f,
                                      auto& var_v_col,
                                      auto& var_lookup,
                                      const size_t local_nrow,
                                      const auto& key_vec)
{
    if constexpr (RsltType) {
                            
        var_v_col.resize(local_nrow);
        CreatePolicy::template apply<Function, CORES>(f, 
                                                      var_v_col, 
                                                      var_lookup,
                                                      local_nrow,
                                                      key_vec);

    } else {
        std::visit([&](auto&& v_ptr, auto&& lookup_ptr) {

            using TP  = std::decay_t<decltype(v_ptr)>;
            using TP2 = std::decay_t<decltype(lookup_ptr)>;

            if constexpr (!std::is_same_v<TP, std::monostate> &&
                          !std::is_same_v<TP2, std::monostate>) {

                auto& value_col = std::get<TP>(var_v_col);
                value_col.resize(local_nrow);

                const auto& lookup = std::get<TP2>(var_lookup);

                CreatePolicy::template apply<Function, 
                                             CORES,
                                             StandardMethod>(f, 
                                                             value_col, 
                                                             lookup,
                                                             local_nrow,
                                                             key_vec);

            }
        }, var_v_col, var_lookup);
    }
}





