# Phase S-1 — kvcache micro-benchmarks.
#
# Each bench is a tiny standalone binary that links against the
# public C ABI (libkvcache.dylib). No external bench framework — we
# use <chrono> + sort. Build with `cmake --build . --target bench`.

add_executable(bench_lookup bench_lookup.cpp)
target_include_directories(bench_lookup PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(bench_lookup PRIVATE kvcache)

add_executable(bench_fetch bench_fetch.cpp)
target_include_directories(bench_fetch PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(bench_fetch PRIVATE kvcache)

# Phase S-2 — multi-tenant fairness bench.
add_executable(bench_fairness bench_fairness.cpp)
target_include_directories(bench_fairness PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(bench_fairness PRIVATE kvcache)

# Phase S-3 — priority-class preemption bench.
add_executable(bench_priority bench_priority.cpp)
target_include_directories(bench_priority PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(bench_priority PRIVATE kvcache)

# Convenience aggregate target.
add_custom_target(bench DEPENDS bench_lookup bench_fetch bench_fairness bench_priority)
