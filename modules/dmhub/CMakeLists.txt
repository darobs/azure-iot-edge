#Copyright (c) Microsoft. All rights reserved.
#Licensed under the MIT license. See LICENSE file in the project root for full license information.

cmake_minimum_required(VERSION 2.8.11)
#this is CMakeLists for the dmhub module

set(dmhub_sources
	./src/dmhub.c
)

set(dmhub_headers
	./inc/dmhub.h
)

set(dmhub_hl_sources
	./src/dmhub_hl.c
)

set(dmhub_hl_headers
	./inc/dmhub_hl.h
)

include_directories(./inc)
include_directories(${GW_INC})
include_directories(${IOTHUB_CLIENT_INC_FOLDER})
include_directories(${IOTDM_CLIENT_INC_FOLDER})
include_directories(${IOTDM_CLIENT_INC_FOLDER}/../lwm2m_objects)


#this builds the dmhub dynamic library
add_library(dmhub MODULE
    ${dmhub_sources}
    ${dmhub_headers}
)
target_link_libraries(dmhub
    gateway
    iotdm_client  
    iothub_client
)

#this builds the dmhub static library
add_library(dmhub_static
    ${dmhub_sources}
    ${dmhub_headers}
)
target_compile_definitions(dmhub_static PRIVATE BUILD_MODULE_TYPE_STATIC)
target_link_libraries(dmhub_static
    gateway
    iotdm_client
    iothub_client
)
linkSharedUtil(dmhub)
linkSharedUtil(dmhub_static)

#this builds the dmhub_HL dynamic library (by default it uses IoTHub Module linked statically)
add_library(dmhub_hl MODULE
    ${dmhub_hl_sources}
    ${dmhub_hl_headers}
)
target_link_libraries(dmhub_hl
    dmhub_static
)

#this builds the dmhub_HL static library (by default it uses IoTHub Module linked statically)
add_library(dmhub_hl_static
    ${dmhub_hl_sources}
    ${dmhub_hl_headers}
)
target_compile_definitions(dmhub_hl_static PRIVATE BUILD_MODULE_TYPE_STATIC)
target_link_libraries(dmhub_hl_static
    dmhub_static
)


linkSharedUtil(dmhub_hl)
linkSharedUtil(dmhub_hl_static)


add_module_to_solution(dmhub)

if(install_executables)
	install(TARGETS dmhub LIBRARY DESTINATION lib) 
    install(TARGETS dmhub_hl LIBRARY DESTINATION lib) 
endif()
