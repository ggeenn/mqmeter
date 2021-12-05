cmake_minimum_required(VERSION 3.0.2) 

project(mqm DESCRIPTION "mqm.")

file(GLOB_RECURSE LIB_SOURCES
    "include/*.h"
    "src/*.cpp"
)

add_library(${PROJECT_NAME} STATIC ${LIB_SOURCES})

target_include_directories(${PROJECT_NAME} PUBLIC "include")

target_link_libraries(${PROJECT_NAME}
    PUBLIC
        CONAN_PKG::boost
)
