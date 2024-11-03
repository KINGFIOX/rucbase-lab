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

#include "common/common.h"
#include "execution_defs.h"
#include "index/ix.h"
#include "system/sm.h"

class AbstractExecutor {
 public:
  Rid _abstract_rid;

  Context *context_;

  // 就是有多态的话, 需要: 虚析构. 防止出现: 子类被部分析构
  virtual ~AbstractExecutor() = default;

  virtual size_t tupleLen() const { return 0; };

  virtual const std::vector<ColMeta> &cols() const {
    std::vector<ColMeta> *_cols = nullptr;
    return *_cols;
  };

  virtual std::string getType() { return "AbstractExecutor"; };

  /* ---------- 迭代器模式 ---------- */

  virtual void beginTuple() {};

  virtual void nextTuple() {};

  virtual bool is_end() const { return true; };

  virtual Rid &rid() = 0;

  virtual std::unique_ptr<RmRecord> Next() = 0;

  virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta(); };

  /* ----------  ---------- */

  // 返回 const_iterator, 是因为: const std::vector<ColMeta> &rec_cols
  std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target) {
    auto pos =
        std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) { return col.tab_name == target.tab_name && col.name == target.col_name; });
    if (pos == rec_cols.end()) {
      throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
    }
    return pos;
  }
};