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
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class ProjectionExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;        // 投影节点的儿子节点
    std::vector<ColMeta> cols_;                     // 需要投影的字段
    size_t len_;                                    // 字段总长度
    std::vector<size_t> sel_idxs_;                  // 需要投影的字段在原表中的位置
    std::vector<RmRecord> recs;    // 用于去重

   public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) {
        prev_ = std::move(prev);

        size_t curr_offset = 0;
        auto &prev_cols = prev_->cols();
        for (auto &sel_col : sel_cols) {
            auto pos = get_col(prev_cols, sel_col);
            sel_idxs_.push_back(pos - prev_cols.begin());
            auto col = *pos;
            col.offset = curr_offset;
            curr_offset += col.len;
            cols_.push_back(col);
        }
        len_ = curr_offset;
    }

    const std::vector<ColMeta> &cols() const override {
        return cols_;
    }

    size_t tupleLen() const override {
        return len_;
    }

    std::string getType() override {
        return "ProjectionExecutor";
    }

    void beginTuple() override {
        prev_->beginTuple();
    }

    void nextTuple() override {
        auto rec = prev_->Next();
        auto new_rec = std::make_unique<RmRecord>(len_);
        intoNewRec(rec, new_rec);
        recs.push_back(*new_rec);
    }

    bool is_end() const override {
        while (true) {
            auto rec = prev_->Next();
            if (prev_->is_end() || rec == nullptr) {
                return true;
            }
            auto new_rec = std::make_unique<RmRecord>(len_);
            intoNewRec(rec, new_rec);
            if (isNotDuplicate(new_rec)){
                return false;
            }
            prev_->nextTuple();
        }
    }

    bool isNotDuplicate(std::unique_ptr<RmRecord> &rec) const {
        for (auto &r : recs) {
            if (memcmp(r.data, rec->data, len_) == 0) {
                return false;
            }
        }
        return true;
    }

    void intoNewRec(std::unique_ptr<RmRecord> &rec, std::unique_ptr<RmRecord> &new_rec) const {
        for (size_t i = 0; i < sel_idxs_.size(); i++) {
            auto &col = cols_[i];
            auto &prev_col = prev_->cols()[sel_idxs_[i]];
            auto rec_buf = rec->data + prev_col.offset;
            auto new_rec_buf = new_rec->data + col.offset;
            if (col.type == TYPE_INT) {
                *(int *)new_rec_buf = *(int *)rec_buf;
            } else if (col.type == TYPE_FLOAT) {
                *(float *)new_rec_buf = *(float *)rec_buf;
            } else if (col.type == TYPE_STRING) {
                memcpy(new_rec_buf, rec_buf, col.len);
            }
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        auto rec = prev_->Next();
        auto new_rec = std::make_unique<RmRecord>(len_);
        intoNewRec(rec, new_rec);
        recs.push_back(*new_rec);
        return new_rec;
    }

    Rid &rid() override { return _abstract_rid; }
};