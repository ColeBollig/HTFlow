cmake_minimum_required(VERSION 3.14)
project(htflow-tests
        VERSION 1.0.0
        DESCRIPTION "Test suite (unit and regression) for HTFLow functionality"
        LANGUAGES NONE)

message(STATUS "Project Name--------:  ${PROJECT_NAME}")
message(STATUS "Project Version-----:  ${PROJECT_VERSION}")
message(STATUS "Project Description-:  ${PROJECT_DESCRIPTION}")
message(STATUS "")

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

# create_test(<file> [LABELS <label>...] [PYTEST_ARGS <arg>...])
#
# LABELS becomes the CTest test's LABELS property, so `ctest -L <label>` /
# `ctest -LE <label>` filters the same way `pytest -m <marker>` /
# `pytest -m "not <marker>"` already does for that file's tests -- keep
# these labels in sync with the marker names in pyproject.toml's
# [tool.pytest.ini_options] markers list.
function(create_test FILE)
    cmake_parse_arguments(ARG "" "" "LABELS;PYTEST_ARGS" ${ARGN})
    get_filename_component(TEST_NAME ${FILE} NAME_WE)
    add_test(
        NAME ${TEST_NAME}
        COMMAND pytest ${CMAKE_CURRENT_SOURCE_DIR}/${FILE} -v ${ARG_PYTEST_ARGS}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
    if(ARG_LABELS)
        set_tests_properties(${TEST_NAME} PROPERTIES LABELS "${ARG_LABELS}")
    endif()
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
    message(STATUS "pytest-xdist not found -- live tests will run sequentially. `pip install pytest-xdist` to parallelize them.")
endif()

create_test(test_dag.py LABELS "unit;fast")
create_test(test_dataflow.py LABELS "unit;fast")
create_test(test_change_directory.py LABELS "unit;fast")
create_test(test_cli.py LABELS "unit;fast")
create_test(test_execute.py LABELS "unit;fast")
create_test(test_sources.py LABELS "unit;fast")
create_test(test_naming.py LABELS "unit;fast")
create_test(test_monitor.py LABELS "integration;live;condor;slow" PYTEST_ARGS ${LIVE_CONDOR_XDIST_ARGS})
create_test(test_submit.py LABELS "integration;live;condor;slow" PYTEST_ARGS ${LIVE_CONDOR_XDIST_ARGS})

