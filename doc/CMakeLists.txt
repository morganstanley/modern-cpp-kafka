find_program (DOXYGEN_EXECUTABLE NAMES doxygen)
if (NOT DOXYGEN_EXECUTABLE)
    message(FATAL_ERROR "Could not find doxygen!")
else()
    message("Doxygen build started... [${DOXYGEN_EXECUTABLE}]")
endif()

set(DOXYGEN_INPUT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../include/kafka)

set(doxyfile_in ${CMAKE_CURRENT_SOURCE_DIR}/../scripts/doxyfile.cfg)
set(doxyfile_out ${CMAKE_CURRENT_BINARY_DIR}/doxyfile.cfg)
configure_file(${doxyfile_in} ${doxyfile_out} @ONLY)

add_custom_target(
    doxygen-doc ALL
    COMMAND ${DOXYGEN_EXECUTABLE} ${doxyfile_out}
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/../include
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Generating doxygen documents"
    VERBATIM
)

install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/doxygen DESTINATION doc)

message("Markdown build started...")
set (DOC_ENTRY_SRC ${CMAKE_CURRENT_SOURCE_DIR}/../README.md)
set (MARKDOWN_EXECUTABLE "${CMAKE_CURRENT_SOURCE_DIR}/../scripts/markdown2html.py")
set (MARKDOWN_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/markdown")
file(GLOB MARKDOWN_INPUT_FILES ${CMAKE_CURRENT_SOURCE_DIR}/*.md)

add_custom_target(
    doc-entry ALL
    COMMAND ${MARKDOWN_EXECUTABLE} -i ${DOC_ENTRY_SRC} -o ${CMAKE_CURRENT_BINARY_DIR}
    DEPENDS ${DOC_ENTRY_SRC}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Generating documents entry"
    VERBATIM
)

add_custom_target(
    markdown-doc ALL
    COMMAND ${MARKDOWN_EXECUTABLE} -i ${MARKDOWN_INPUT_FILES} -o ${MARKDOWN_OUTPUT_DIR}
    DEPENDS ${MARKDOWN_INPUT_FILES}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Generating markdown documents"
    VERBATIM
)

install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/markdown DESTINATION doc)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/index.html DESTINATION doc)
