cmake_minimum_required(VERSION 3.20)
project(mqmeter DESCRIPTION "Mqm task processor.")

if(NOT EXISTS "${CMAKE_CURRENT_BINARY_DIR}/conan.cmake")
    message(STATUS "Downloading conan.cmake from https://github.com/conan-io/cmake-conan")
    file(DOWNLOAD "https://raw.githubusercontent.com/conan-io/cmake-conan/master/conan.cmake"
                  "${CMAKE_CURRENT_BINARY_DIR}/conan.cmake")

endif()
include(${CMAKE_CURRENT_BINARY_DIR}/conan.cmake)

conan_cmake_run(CONANFILE conanfile.py
                BASIC_SETUP CMAKE_TARGETS
                BUILD missing)

include(${CMAKE_CURRENT_BINARY_DIR}/conanbuildinfo.cmake)
include(${CMAKE_CURRENT_BINARY_DIR}/conan_paths.cmake)

add_subdirectory(mqm)
add_subdirectory(mqm_tst)
