# Prototype C implementation of vshard.storage.call. Requires
# tarantool >= 3.9 with box_func_call_by_name() (see the plan in
# the repo root). Extra include dirs for an uninstalled tarantool
# source tree can be passed via TARANTOOL_SRC_DIR.
if(DEFINED TARANTOOL_SRC_DIR)
    include_directories(
        ${TARANTOOL_SRC_DIR}/src
        ${TARANTOOL_SRC_DIR}/src/lib/msgpuck
        ${TARANTOOL_SRC_DIR}/third_party/luajit/src)
endif()

add_library(storage_c SHARED storage_c.c refs.c)
set_target_properties(storage_c PROPERTIES
    PREFIX ""
    OUTPUT_NAME storage_c
    C_VISIBILITY_PRESET hidden
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/vshard)
install(TARGETS storage_c LIBRARY
        DESTINATION ${TARANTOOL_INSTALL_LIBDIR}/vshard)
