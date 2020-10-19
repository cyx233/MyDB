add_library(test_suite ${CMAKE_SOURCE_DIR}/src/const.h)
target_link_libraries(test_suite gtest gtest_main ${CMAKE_PROJECT_NAME}_lib)

add_executable(db_test db_test.cc)
target_link_libraries(db_test test_suite)
add_test(NAME DatabaseTest COMMAND db_test)

add_executable(table_test table_test.cc)
target_link_libraries(table_test test_suite)
add_test(NAME TableTest COMMAND table_test)