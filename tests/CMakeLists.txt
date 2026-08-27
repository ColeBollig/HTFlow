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
        COMMAND pytest ${CMAKE_CURRENT_SOURCE_DIR}/${FILE} -v ${ARGN}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endfunction()

# Parallelize live-Schedd tests if pytest-xdist is available (optional --
# falls back to sequential otherwise). --collect-only forces real plugin
# loading; --version alone always reports success even if xdist is missing.
execute_process(
    COMMAND pytest -p xdist --collect-only -q ${CMAKE_CURRENT_SOURCE_DIR}/test_dag.py
    RESULT_VARIABLE XDIST_MISSING
    OUTPUT_QUIET
    ERROR_QUIET
)

if(XDIST_MISSING EQUAL 0)
    set(LIVE_CONDOR_XDIST_ARGS -n auto)
else()
    set(LIVE_CONDOR_XDIST_ARGS "")
    message(STATUS "pytest-xdist not found -- live_condor tests will run sequentially. `pip install pytest-xdist` to parallelize them.")
endif()

create_test(test_dag.py)
create_test(test_dataflow.py)
create_test(test_change_directory.py)
create_test(test_cli.py)
create_test(test_execute.py)
create_test(test_sources.py)
create_test(test_naming.py)
create_test(test_monitor.py ${LIVE_CONDOR_XDIST_ARGS})
create_test(test_submit.py ${LIVE_CONDOR_XDIST_ARGS})

