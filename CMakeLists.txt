cmake_minimum_required (VERSION 2.8.2)
project (PTC)
message (STATUS "Configuring PTC")

# ----------------------------------------------------------------------------
# Dependencies
# ----------------------------------------------------------------------------
set (CXX_STANDARD 14)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Boost 1.36.0)
if(NOT Boost_FOUND)
    message (STATUS "  ptc cannot be built without Boost.")
    return ()
endif()

# ----------------------------------------------------------------------------
# Build Setup
# ----------------------------------------------------------------------------

# Update the list of file names below if you add source files to your application.
#target_include_directories (tests PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
#add_library (tests tests/tests.cpp)
add_executable(tests tests/tests.cpp ptc/ptc.h)

set_property(TARGET tests PROPERTY CXX_STANDARD 14)
include_directories(${Boost_INCLUDE_DIRS})
target_link_libraries(tests ${Boost_LIBRARIES})

# Set CXX flags 
if (NOT MSVC)
set (CMAKE_CXX_FLAGS "-pthread ${CMAKE_CXX_FLAGS}")
endif (NOT MSVC)
