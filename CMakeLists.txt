cmake_minimum_required(VERSION 3.5 FATAL_ERROR)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})
include(ExternalProject)

project(vshard C)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

# Find Tarantool
set(TARANTOOL_FIND_REQUIRED ON)
set(CMAKE_INSTALL_DATADIR "" )
find_package(Tarantool)
include_directories(${TARANTOOL_INCLUDE_DIRS})

add_definitions("-D_GNU_SOURCE")

# Set CFLAGS
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=gnu99")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Wall -Wextra")

# The C accelerator for vshard.storage.call. It needs the cdata API
# (luaL_iscdata & co) in the core's module.h. When the header is too old,
# silently build the pure-Lua vshard - a missing call_c.so only means no
# acceleration.
option(VSHARD_ENABLE_C "Build the C accelerator for vshard.storage.call" ON)
if(VSHARD_ENABLE_C)
    file(READ "${TARANTOOL_INCLUDE_DIR}/tarantool/module.h" _module_h)
    string(FIND "${_module_h}" "luaL_iscdata" _cdata_api_pos)
    unset(_module_h)
    if(_cdata_api_pos EQUAL -1)
        message(WARNING "tarantool/module.h lacks the cdata API - "
                "the C accelerator is disabled")
        set(VSHARD_ENABLE_C OFF)
    endif()
endif()

# Build module
add_subdirectory(vshard)
# Enable tests
add_subdirectory(test)
