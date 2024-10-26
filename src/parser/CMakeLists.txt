find_package(BISON REQUIRED)
find_package(FLEX REQUIRED)

# TARGET yacc
# SRC yacc.y
# output ${CMAKE_CURRENT_SOURCE_DIR}/yacc.tab.cpp, 指定了生成的 cpp 文件的路径
# DEFINES_FILE 指定了生成的头文件的路径, including the defination of the parser
bison_target(yacc yacc.y ${CMAKE_CURRENT_SOURCE_DIR}/yacc.tab.cpp
        DEFINES_FILE ${CMAKE_CURRENT_SOURCE_DIR}/yacc.tab.h)
flex_target(lex lex.l ${CMAKE_CURRENT_SOURCE_DIR}/lex.yy.cpp)
# make sure yacc depends on lex
add_flex_bison_dependency(lex yacc)

set(SOURCES ${BISON_yacc_OUTPUT_SOURCE} ${FLEX_lex_OUTPUTS} ast.cpp)
add_library(parser STATIC ${SOURCES})

add_executable(test_parser test_parser.cpp)
target_link_libraries(test_parser parser)
add_test(NAME test_parser COMMAND test_parser
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
