#pragma once

template <typename T>
struct is_reserving_vec : std::false_type {};

template <typename U>
struct is_reserving_vec<ReservingVec<U>> : std::true_type {};
