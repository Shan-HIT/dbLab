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

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
    }

    const std::vector<ColMeta> &cols() const override {
        return cols_;
    };

    size_t tupleLen() const override {
        return len_;
    }

    std::string getType() override {
        return "SeqScanExecutor";
    }

    bool isSatisfy (Rid rid) {
        auto rec = fh_->get_record(rid, context_); // 获取记录
        for (auto &cond : conds_) { // 遍历所有的条件
            if (cond.is_rhs_val) { // 如果右侧是值
                for (auto &col : cols_) { // 遍历所有的字段
                    if (col.name == cond.lhs_col.col_name) { // 找到左侧的字段
                        Value lhs = toValue(*rec, col); // 获取左侧的值
                        if (!compareBtw(lhs, cond.rhs_val, cond.op)) { // 比较左右两侧的值
                            return false;
                        }
                    }
                }
            } else {
                Value lhs, rhs;
                for (auto &col : cols_) {
                    if (col.name == cond.lhs_col.col_name) {
                        lhs = toValue(*rec, col);
                    }
                    if (col.name == cond.rhs_col.col_name) {
                        rhs = toValue(*rec, col);
                    }
                }
                if (!compareBtw(lhs, rhs, cond.op)) {
                    return false;
                }
            }
        }
        return true;
    }

    Value toValue(const RmRecord &rec, ColMeta &col) {
        Value val;
        switch (col.type) {
            case TYPE_INT:
                val.type = TYPE_INT;
                val.int_val = *reinterpret_cast<int *>(rec.data + col.offset);
                break;
            case TYPE_FLOAT:
                val.type = TYPE_FLOAT;
                val.float_val = *reinterpret_cast<float *>(rec.data + col.offset);
                break;
            case TYPE_STRING:
                val.type = TYPE_STRING;
                val.str_val = std::string(rec.data + col.offset, col.len);
                break;
            default:
                assert(false);
        }
        return val;
    }

    bool compareBtw(const Value &lhs, const Value &rhs, CompOp op) {
        switch (lhs.type) {
            case TYPE_INT:
                switch (op) {
                    case OP_EQ:
                        return lhs.int_val == rhs.int_val;
                    case OP_LT:
                        return lhs.int_val < rhs.int_val;
                    case OP_GT:
                        return lhs.int_val > rhs.int_val;
                    case OP_LE:
                        return lhs.int_val <= rhs.int_val;
                    case OP_GE:
                        return lhs.int_val >= rhs.int_val;
                    case OP_NE:
                        return lhs.int_val != rhs.int_val;
                    default:
                        assert(false);
                }
            case TYPE_FLOAT:
                switch (op) {
                    case OP_EQ:
                        return lhs.float_val == rhs.float_val;
                    case OP_LT:
                        return lhs.float_val < rhs.float_val;
                    case OP_GT:
                        return lhs.float_val > rhs.float_val;
                    case OP_LE:
                        return lhs.float_val <= rhs.float_val;
                    case OP_GE:
                        return lhs.float_val >= rhs.float_val;
                    case OP_NE:
                        return lhs.float_val != rhs.float_val;
                    default:
                        assert(false);
                }
            case TYPE_STRING:
                switch (op) {
                    case OP_EQ:
                        return lhs.str_val == rhs.str_val;
                    case OP_LT:
                        return lhs.str_val < rhs.str_val;
                    case OP_GT:
                        return lhs.str_val > rhs.str_val;
                    case OP_LE:
                        return lhs.str_val <= rhs.str_val;
                    case OP_GE:
                        return lhs.str_val >= rhs.str_val;
                    case OP_NE:
                        return lhs.str_val != rhs.str_val;
                    default:
                        assert(false);
                }
            default:
                assert(false);
        }
    }

    /**
     * @brief 构建表迭代器scan_,并开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void beginTuple() override {
        scan_ = std::unique_ptr<RmScan>(new RmScan(fh_));
        while(!scan_->is_end() && !isSatisfy(scan_->rid())) {
            scan_->next();
        }
        if (!scan_->is_end()) {
            rid_ = scan_->rid();
        }
    }

    /**
     * @brief 从当前scan_指向的记录开始迭代扫描,直到扫描到下一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void nextTuple() override {
        scan_->next();
        while(!scan_->is_end() && !isSatisfy(scan_->rid())) {
            scan_->next();
        }
        if (!scan_->is_end()) {
            rid_ = scan_->rid();
        }
    }

    bool is_end() const override {
        return scan_->is_end();
    }

    /**
     * @brief 返回下一个满足扫描条件的记录
     *
     * @return std::unique_ptr<RmRecord>
     */
    std::unique_ptr<RmRecord> Next() override {
        return fh_->get_record(rid_, context_);
    }

    Rid &rid() override { return rid_; }

    ColMeta get_col_offset(const TabCol &target) override { // ???
        auto pos = std::find_if(cols_.begin(), cols_.end(), [&](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (pos == cols_.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return *pos;
    }
};