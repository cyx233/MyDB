set(HEADERS
        ${HEADERS}
        ${CMAKE_CURRENT_SOURCE_DIR}/database.h
        ${CMAKE_CURRENT_SOURCE_DIR}/header_page.h
        ${CMAKE_CURRENT_SOURCE_DIR}/table.h
        ${CMAKE_CURRENT_SOURCE_DIR}/type.h
        PARENT_SCOPE
        )

set(SOURCE
        ${SOURCE}
        ${CMAKE_CURRENT_SOURCE_DIR}/database.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/table.cc
        ${CMAKE_CURRENT_SOURCE_DIR}/type.cc
        PARENT_SCOPE
        )