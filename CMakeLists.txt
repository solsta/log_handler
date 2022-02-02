cmake_minimum_required(VERSION 3.21)
project(log_handler C)

set(CMAKE_C_STANDARD 11)
link_libraries(pmemlog pmemobj)

add_executable(log_handler main.c)
