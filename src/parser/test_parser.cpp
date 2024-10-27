/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */
#undef NDEBUG

#include <cassert>

#include "parser.h"

int main() {
  std::vector<std::string> sqls = {
      "show tables;",
      "desc tb;",
      "create table tb (a int, b float, c char(4));",
      "drop table tb;",
      "create index tb(a);",
      "create index tb(a, b, c);",
      "drop index tb(a, b, c);",
      "drop index tb(b);",
      "insert into tb values (1, 3.14, 'pi');",
      "delete from tb where a = 1;",
      "update tb set a = 1, b = 2.2, c = 'xyz' where x = 2 and y < 1.1 and z > 'abc';",
      "select * from tb;",
      "select * from tb where x <> 2 and y >= 3. and z <= '123' and b < tb.a;",
      "select x.a, y.b from x, y where x.a = y.b and c = d;",
      "select x.a, y.b from x join y where x.a = y.b and c = d;",
      "exit;",
      "help;",
      "",
  };
  for (auto &sql : sqls) {
    std::cout << sql << std::endl;
    // 启动语法分析过程, 根据 bison 定义的语法规则解析输入, 返回 buf
    YY_BUFFER_STATE buf = yy_scan_string(sql.c_str());
    assert(yyparse() == 0);
    if (ast::parse_tree != nullptr) {
      // parse_tree 是一个全局变量, src/parser/ast.cpp 中定义, yacc.y
      ast::TreePrinter::print(ast::parse_tree);
      // 释放 buf, 释放资源
      yy_delete_buffer(buf);
      std::cout << std::endl;  // newline and flush
    } else {
      std::cout << "exit/EOF" << std::endl;
    }
  }
  ast::parse_tree.reset();  // descries the refcnt
  return 0;
}
