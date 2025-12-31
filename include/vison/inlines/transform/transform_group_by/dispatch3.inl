#pragma once

template <GroupFunction Function,
          unsigned int Nb,
          typename TContainer,
          typename FunctionLoop,
          typename FunctionKey
         >
inline void dispatch3(const size_t start,
                      const size_t end,
                      auto& cmap,
                      const auto& key_col,
                      const size_t val_size,
                      const auto& val_col,
                      const size_t NPerGroup,
                      std::vector<unsigned int>& key_idx,
                      const std::vector<unsigned int>& idx_str,
                      const std::vector<unsigned int>& idx_chr,
                      const std::vector<unsigned int>& idx_bool,
                      const std::vector<unsigned int>& idx_int,
                      const std::vector<unsigned int>& idx_uint,
                      const std::vector<unsigned int>& idx_dbl,
                      auto& key_vec
                      ) 
{

    cmap.reserve((end - start) / NPerGroup);
    using Elem = typename std::decay_t<decltype(val_col)>::value_type;

    if constexpr (Nb == 2) {

        ReservingVec<unsigned int> vec(NPerGRoup);
        FunctionLoop::template apply<TContainer, FunctionKey>(start, 
                                                              end, 
                                                              cmap, 
                                                              vec, 
                                                              key_col, 
                                                              val_size,
                                                              val_col,
                                                              key_idx,
                                                              idx_str,
                                                              idx_chr,
                                                              idx_bool,
                                                              idx_int,
                                                              idx_uint,
                                                              idx_dbl,
                                                              key_vec
                                                              );

        return;
    }

    if constexpr (Function == GroupFunction::Occurence) {

        if constexpr (Nb == 0) { // normal

            UIntT val = 0;
            FunctionLoop::template apply<TContainer, FunctionKey>(start, 
                                                                  end, 
                                                                  cmap, 
                                                                  val, 
                                                                  key_col, 
                                                                  val_size,
                                                                  val_col,
                                                                  key_idx,
                                                                  idx_str,
                                                                  idx_chr,
                                                                  idx_bool,
                                                                  idx_int,
                                                                  idx_uint,
                                                                  idx_dbl,
                                                                  key_vec
                                                                  );

        } else if constexpr (Nb == 1) { // hard

            PairGroupBy<UIntT> val_struct(NPerGRoup);
            FunctionLoop::template apply<TContainer, FunctionKey>(start, 
                                                                  end, 
                                                                  cmap, 
                                                                  val_struct, 
                                                                  key_col, 
                                                                  val_size,
                                                                  val_col,
                                                                  key_idx,
                                                                  idx_str,
                                                                  idx_chr,
                                                                  idx_bool,
                                                                  idx_int,
                                                                  idx_uint,
                                                                  idx_dbl,
                                                                  key_vec
                                                                  );

        }

    } else if constexpr (Function != GroupFunction::Gather) {

        if constexpr (Nb == 0) {

            Elem val{};
            FunctionLoop::template apply<TContainer, FunctionKey>(val_col, 
                                                                  start, 
                                                                  end, 
                                                                  cmap, 
                                                                  val, 
                                                                  key_col, 
                                                                  val_size,
                                                                  key_idx,
                                                                  idx_str,
                                                                  idx_chr,
                                                                  idx_bool,
                                                                  idx_int,
                                                                  idx_uint,
                                                                  idx_dbl,
                                                                  key_vec
                                                                  );

        } else {

            PairGroupBy<Elem> val_struct(NPerGRoup);
            FunctionLoop::template apply<TContainer, FunctionKey>(val_col, 
                                                                  start, 
                                                                  end, 
                                                                  cmap, 
                                                                  val_struct, 
                                                                  key_col, 
                                                                  val_size,
                                                                  key_idx,
                                                                  idx_str,
                                                                  idx_chr,
                                                                  idx_bool,
                                                                  idx_int,
                                                                  idx_uint,
                                                                  idx_dbl,
                                                                  key_vec
                                                                  );

        }

    } else {

        if constexpr (Nb == 0) {

            ReservingVec<Elem> vec(NPerGRoup);
            FunctionLoop::template apply<TContainer, FunctionKey>(val_col, 
                                                                  start, 
                                                                  end, 
                                                                  cmap, 
                                                                  vec, 
                                                                  key_col, 
                                                                  val_size,
                                                                  key_idx,
                                                                  idx_str,
                                                                  idx_chr,
                                                                  idx_bool,
                                                                  idx_int,
                                                                  idx_uint,
                                                                  idx_dbl,
                                                                  key_vec
                                                                  );

        } else {

            PairGroupBy<ReservingVec<Elem>> vec_struct(NPerGRoup);
            FunctionLoop::template apply<TContainer, FunctionKey>(val_col, 
                                                                  start, 
                                                                  end, 
                                                                  cmap, 
                                                                  vec_struct, 
                                                                  key_col, 
                                                                  val_size,
                                                                  key_idx,
                                                                  idx_str,
                                                                  idx_chr,
                                                                  idx_bool,
                                                                  idx_int,
                                                                  idx_uint,
                                                                  idx_dbl,
                                                                  key_vec
                                                                  );

        }

    }
};





