add_executable(test_db test_db.cc)
target_link_libraries(test_db leveldb)

add_executable(test_code test_code.cc)
target_link_libraries(test_code leveldb)