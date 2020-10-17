add_library(test_suite ${CMAKE_SOURCE_DIR}/src/const.h)
target_link_libraries(test_suite gtest gtest_main ${CMAKE_PROJECT_NAME}_lib)

add_executable(table_test table_test.cc)
target_link_libraries(table_test test_suite)
add_test(NAME TestTable COMMAND table_test)