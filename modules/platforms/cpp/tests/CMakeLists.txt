include_directories(${Boost_INCLUDE_DIRS})

# This function makes executable from given source file with Boost
# test cases and registers them as individual CTest test cases.
function(add_boost_test SOURCE_FILE DEPENDENCIES)
    get_filename_component(EXECUTABLE_NAME ${SOURCE_FILE} NAME_WE)

    string(REGEX REPLACE "^test" "" TEST_MODULE ${EXECUTABLE_NAME})

    add_executable(${EXECUTABLE_NAME} ${SOURCE_FILE})

    target_compile_definitions(${EXECUTABLE_NAME} PUBLIC -DBOOST_TEST_MODULE=${TEST_MODULE} -DBOOST_TEST_DYN_LINK)

    target_link_libraries(${EXECUTABLE_NAME} Boost::unit_test_framework ${DEPENDENCIES})

    target_link_libraries(${EXECUTABLE_NAME} $<$<AND:$<CXX_COMPILER_ID:GNU>,$<VERSION_LESS:$<CXX_COMPILER_VERSION>,8.0>>:stdc++fs>)

    file(READ ${SOURCE_FILE} SOURCE)

    string(REGEX MATCHALL "BOOST_(AUTO|FIXTURE)_TEST_CASE\\(([A-Za-z_0-9_,_ ]+)\\)" FOUND_TESTS ${SOURCE})

    foreach(TEST ${FOUND_TESTS})
        string(REGEX REPLACE ".*\\( *([A-Za-z_0-9]+).*" "\\1" TEST_NAME ${TEST})

        add_test(NAME ${EXECUTABLE_NAME}.${TEST_NAME} COMMAND ${EXECUTABLE_NAME} -t ${TEST_NAME}
                 WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
    endforeach()
endfunction()

add_boost_test(testBits.cpp ignite-common)
add_boost_test(testBytes.cpp ignite-common)
