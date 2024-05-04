set(SOURCES lru_replacer.cpp)
add_library(lru_replacer STATIC ${SOURCES})
set(CMAKE_CXX_STANDARD 17)  # 设置 C++ 标准版本
set(CMAKE_CXX_FLAGS "-Wall -Wextra")  # 添加编译器标志，如 -Wall 和 -Wextra