#Copyright (c) Microsoft. All rights reserved.
#Licensed under the MIT license. See LICENSE file in the project root for full license information.

cmake_minimum_required(VERSION 2.8.11)
#this is CMakeLists for the dm_worker module

set(dm_worker_sources
	./src/dm_worker.c
)

set(dm_worker_headers
	./inc/dm_worker.h
)


include_directories(./inc)
include_directories(${GW_INC})

#this builds the dm_worker dynamic library
add_library(dm_worker MODULE ${dm_worker_sources}  ${dm_worker_headers})
target_link_libraries(dm_worker gateway)

#this builds the dm_worker static library
add_library(dm_worker_static  ${dm_worker_sources} ${dm_worker_headers})
target_compile_definitions(dm_worker_static PRIVATE BUILD_MODULE_TYPE_STATIC)
target_link_libraries(dm_worker_static gateway)


linkSharedUtil(dm_worker)
linkSharedUtil(dm_worker_static)

add_module_ll_to_solution(dm_worker)

if(install_executables)
	install(TARGETS dm_worker LIBRARY DESTINATION lib) 
endif()
