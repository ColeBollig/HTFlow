cmake_minimum_required(VERSION 3.14)
project(dataflow2dag LANGUAGES NONE)

enable_testing()

find_package(Python3 REQUIRED COMPONENTS Interpreter)

execute_process(
    COMMAND pytest --version
    RESULT_VARIABLE PYTEST_FOUND
    OUTPUT_QUIET
    ERROR_QUIET
)

if(NOT PYTEST_FOUND EQUAL 0)
    message(FATAL_ERROR "pytest not found.")
endif()

function(create_test FILE)
    get_filename_component(TEST_NAME ${FILE} NAME_WE)
    add_test(
        NAME ${TEST_NAME}
        COMMAND pytest ${CMAKE_CURRENT_SOURCE_DIR}/${FILE} -v
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endfunction()

create_test(test_dag.py)
create_test(test_dataflow.py)
create_test(test_change_directory.py)
create_test(test_cli.py)
create_test(test_execute.py)

