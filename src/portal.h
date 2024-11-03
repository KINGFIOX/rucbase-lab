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

#include <cerrno>
#include <cstring>
#include <string>

#include "common/common.h"
#include "execution/execution_sort.h"
#include "execution/executor_abstract.h"
#include "execution/executor_delete.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_insert.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_update.h"
#include "optimizer/plan.h"

typedef enum portalTag { PORTAL_Invalid_Query = 0, PORTAL_ONE_SELECT, PORTAL_DML_WITHOUT_SELECT, PORTAL_MULTI_QUERY, PORTAL_CMD_UTILITY } portalTag;

struct PortalStmt {
  portalTag tag;

  std::vector<TabCol> sel_cols;
  std::unique_ptr<AbstractExecutor> root;
  std::shared_ptr<Plan> plan;

  PortalStmt(portalTag tag_, std::vector<TabCol> sel_cols_, std::unique_ptr<AbstractExecutor> root_, std::shared_ptr<Plan> plan_)
      : tag(tag_), sel_cols(std::move(sel_cols_)), root(std::move(root_)), plan(std::move(plan_)) {}
};

// portal means: 入口

class Portal {
 private:
  SmManager *sm_manager_;

 public:
  Portal(SmManager *sm_manager) : sm_manager_(sm_manager) {}
  ~Portal() {}

  // 将查询执行计划转换成对应的算子树
  std::shared_ptr<PortalStmt> start(std::shared_ptr<Plan> plan, Context *context) const {
    // 这里可以将select进行拆分，例如：一个select，带有return的select等
    if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
      return std::make_shared<PortalStmt>(PORTAL_CMD_UTILITY, std::vector<TabCol>(), std::unique_ptr<AbstractExecutor>(), plan);
    } else if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {
      return std::make_shared<PortalStmt>(PORTAL_MULTI_QUERY, std::vector<TabCol>(), std::unique_ptr<AbstractExecutor>(), plan);
    } else if (auto x = std::dynamic_pointer_cast<DMLPlan>(plan)) {
      switch (x->tag) {
        case T_select: {
          // select 是投影
          std::shared_ptr<ProjectionPlan> p = std::dynamic_pointer_cast<ProjectionPlan>(x->subplan_);
          std::unique_ptr<AbstractExecutor> root = convert_plan_executor(p, context);
          return std::make_shared<PortalStmt>(PORTAL_ONE_SELECT, std::move(p->sel_cols_), std::move(root), plan);
        }

        case T_Update: {
          // scan 是从数据源读取数据
          std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->subplan_, context);
          std::vector<Rid> rids;  // record id
          for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
            rids.push_back(scan->rid());
          }
          std::unique_ptr<AbstractExecutor> root = std::make_unique<UpdateExecutor>(sm_manager_, x->tab_name_, x->set_clauses_, x->conds_, rids, context);
          return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
        }
        case T_Delete: {
          std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->subplan_, context);
          std::vector<Rid> rids;
          for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
            rids.push_back(scan->rid());
          }

          std::unique_ptr<AbstractExecutor> root = std::make_unique<DeleteExecutor>(sm_manager_, x->tab_name_, x->conds_, rids, context);

          return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
        }

        case T_Insert: {
          std::unique_ptr<AbstractExecutor> root = std::make_unique<InsertExecutor>(sm_manager_, x->tab_name_, x->values_, context);

          return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
        }

        default:
          throw InternalError("Unexpected field type");
          break;
      }
    } else {
      throw InternalError("Unexpected field type");
    }
    return nullptr;
  }

  /**
   * @brief 遍历算子树并执行算子生成执行结果
   *
   * @param portal
   * @param ql
   * @param txn_id
   * @param context
   *
   * @throw InternalError
   */
  void run(std::shared_ptr<PortalStmt> portal, QlManager *ql, txn_id_t *txn_id, Context *context) {
    switch (portal->tag) {
      case PORTAL_ONE_SELECT: {
        ql->select_from(std::move(portal->root), std::move(portal->sel_cols), context);
        break;
      }

      case PORTAL_DML_WITHOUT_SELECT: {
        // 这里要 std::move, 是因为 portal->root 是 unique_ptr. unique_ptr 的构造函数只接受右值
        ql->run_dml(std::move(portal->root));
        break;
      }
      case PORTAL_MULTI_QUERY: {
        ql->run_mutli_query(portal->plan, context);
        break;
      }
      case PORTAL_CMD_UTILITY: {
        ql->run_cmd_utility(portal->plan, txn_id, context);
        break;
      }
      default: {
        throw InternalError("Unexpected field type");
      }
    }
  }

  // 清空资源
  void drop() {
    // do nothing
  }

  // executor(算子)
  std::unique_ptr<AbstractExecutor> convert_plan_executor(std::shared_ptr<Plan> plan, Context *context) const {
    if (auto x = std::dynamic_pointer_cast<ProjectionPlan>(plan)) {
      // 有点链表的意思. Projection: 投影
      return std::make_unique<ProjectionExecutor>(convert_plan_executor(x->subplan_, context), x->sel_cols_);
    } else if (auto x = std::dynamic_pointer_cast<ScanPlan>(plan)) {
      if (x->tag == T_SeqScan) {
        return std::make_unique<SeqScanExecutor>(sm_manager_, x->tab_name_, x->conds_, context);
      } else {
        return std::make_unique<IndexScanExecutor>(sm_manager_, x->tab_name_, x->conds_, x->index_col_names_, context);
      }
    } else if (auto x = std::dynamic_pointer_cast<JoinPlan>(plan)) {
      std::unique_ptr<AbstractExecutor> left = convert_plan_executor(x->left_, context);
      std::unique_ptr<AbstractExecutor> right = convert_plan_executor(x->right_, context);
      std::unique_ptr<AbstractExecutor> join = std::make_unique<NestedLoopJoinExecutor>(std::move(left), std::move(right), std::move(x->conds_));
      return join;
    } else if (auto x = std::dynamic_pointer_cast<SortPlan>(plan)) {
      return std::make_unique<SortExecutor>(convert_plan_executor(x->subplan_, context), x->sel_col_, x->is_desc_);
    }
    return nullptr;
  }
};