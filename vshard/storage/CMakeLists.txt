install(FILES init.lua reload_evolution.lua ref.lua sched.lua schema.lua
        export_log.lua exports.lua route_dispenser.lua
        DESTINATION ${TARANTOOL_INSTALL_LUADIR}/vshard/storage)

if(VSHARD_ENABLE_C)
    add_library(storage_call_c MODULE call_c.c)
    set_target_properties(storage_call_c PROPERTIES
        PREFIX ""
        OUTPUT_NAME call_c
        C_VISIBILITY_PRESET hidden)
    install(TARGETS storage_call_c LIBRARY
            DESTINATION ${TARANTOOL_INSTALL_LIBDIR}/vshard/storage)
endif()
