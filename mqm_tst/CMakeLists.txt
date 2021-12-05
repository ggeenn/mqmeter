project(mqm_tst DESCRIPTION "mqm test.")

add_executable(${PROJECT_NAME} mqm_tst.cpp)

target_link_libraries(${PROJECT_NAME} PRIVATE mqm)

#add_test(NAME ${PROJECT_NAME} COMMAND $<TARGET_FILE:${PROJECT_NAME}>)
