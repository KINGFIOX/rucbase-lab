/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <map>

#include "common/context.h"
#include "errors.h"
#include "execution/execution.h"
#include "parser/parser.h"
#include "plan.h"
#include "planner.h"
#include "system/sm.h"
#include "transaction/transaction_manager.h"

//! # 查询执行计划 (Query Execution Plan)
//! 执行计划是数据库系统在接收到查询（如 SQL 语句）后，如何有效地执行该查询的一系列步骤的详细描述。它包括以下几个主要阶段：
//! 1. 解析（Parsing）：将查询语句解析为内部表示形式，如抽象语法树（AST）。
//! 2. 优化（Optimization）：选择最优的查询执行策略，可能涉及选择合适的索引、确定连接顺序、决定访问路径等。
//! 3. 生成计划（Plan Generation）：基于优化结果，生成一个具体的执行步骤序列。
//! 4. 执行（Execution）：按照计划执行查询，检索或修改数据。
//! # 命名为 Plan
//! 是因为它代表了执行查询的“计划”。类似于项目管理中的计划，执行计划详细说明了完成任务所需的步骤和资源。
//! 在数据库系统中，执行计划决定了如何高效地访问和操作数据，以满足查询需求

class Optimizer {
 private:
  SmManager *sm_manager_;
  Planner *planner_;

 public:
  Optimizer(SmManager *sm_manager, Planner *planner) : sm_manager_(sm_manager), planner_(planner) {}

  std::shared_ptr<Plan> plan_query(std::shared_ptr<Query> query, Context *context) {
    if (auto x = std::dynamic_pointer_cast<ast::Help>(query->parse)) {
      // help;
      return std::make_shared<OtherPlan>(T_Help, std::string());
    } else if (auto x = std::dynamic_pointer_cast<ast::ShowTables>(query->parse)) {
      // show tables;
      return std::make_shared<OtherPlan>(T_ShowTable, std::string());
    } else if (auto x = std::dynamic_pointer_cast<ast::DescTable>(query->parse)) {
      // desc table;
      return std::make_shared<OtherPlan>(T_DescTable, x->tab_name);
    } else if (auto x = std::dynamic_pointer_cast<ast::TxnBegin>(query->parse)) {
      // begin;
      return std::make_shared<OtherPlan>(T_Transaction_begin, std::string());
    } else if (auto x = std::dynamic_pointer_cast<ast::TxnAbort>(query->parse)) {
      // abort;
      return std::make_shared<OtherPlan>(T_Transaction_abort, std::string());
    } else if (auto x = std::dynamic_pointer_cast<ast::TxnCommit>(query->parse)) {
      // commit;
      return std::make_shared<OtherPlan>(T_Transaction_commit, std::string());
    } else if (auto x = std::dynamic_pointer_cast<ast::TxnRollback>(query->parse)) {
      // rollback;
      return std::make_shared<OtherPlan>(T_Transaction_rollback, std::string());
    } else {
      return planner_->do_planner(query, context);
    }
  }
};
