// /* Copyright (c) 2023 Renmin University of China
// RMDB is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//         http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details. */

// #pragma once

// #include "execution_defs.h"
// #include "execution_manager.h"
// #include "executor_abstract.h"
// #include "index/ix.h"
// #include "system/sm.h"

// class SeqScanExecutor : public AbstractExecutor {
//    private:
//     std::string tab_name_;              // 表的名称
//     std::vector<Condition> conds_;      // scan的条件
//     RmFileHandle *fh_;                  // 表的数据文件句柄
//     std::vector<ColMeta> cols_;         // scan后生成的记录的字段
//     size_t len_;                        // scan后生成的每条记录的长度
//     std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

//     Rid rid_;
//     std::unique_ptr<RecScan> scan_;     // table_iterator

//     SmManager *sm_manager_;

// public:

//     SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
//         sm_manager_ = sm_manager;
//         tab_name_ = std::move(tab_name);
//         conds_ = std::move(conds);
//         TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
//         fh_ = sm_manager_->fhs_.at(tab_name_).get();
//         cols_ = tab.cols;
//         len_ = cols_.back().offset + cols_.back().len;

//         context_ = context;

//         fed_conds_ = conds_;
//     }

//     const std::vector<ColMeta> &cols() const override { return cols_; };

//     size_t tupleLen() const override { return len_; }

//     /**
//      * @brief 构建表迭代器scan_,并开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
//      *
//      */
//     void beginTuple() override {
//         scan_ = std::make_unique<RmScan>(fh_);
//         RmRecord rec(fh_->get_file_hdr().record_size);
//         while (!scan_->is_end() && !eval_conds(cols_, conds_, &rec)) {
//             if (!scan_->is_end()) 
//                 rid_ = scan_->rid();
//         }
//     }

//     /**
//      * @brief 从当前scan_指向的记录开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
//      *
//      */
//     void nextTuple() override {
//         RmRecord rec(fh_->get_file_hdr().record_size);
//         for(scan_->next();!scan_->is_end(); scan_->next()){
//             fh_->get_record(rid_, context_);
//             if (eval_conds(cols_, conds_, &rec)) {
//                 break;
//             }
//         }
//         rid_ = scan_->rid();
//     }


//     /**
//      * @brief 返回下一个满足扫描条件的记录
//      *
//      * @return std::unique_ptr<RmRecord>
//      */
//     std::unique_ptr<RmRecord> Next() override {
//         // if (scan_->is_end()) {
//         //     throw std::runtime_error("No more records");
//         // }
//         RmRecord rec(fh_->get_file_hdr().record_size);
//         fh_->get_record(rid_,context_);
//         // if (!eval_conds(cols_, conds_, &rec)) {
//         //     throw std::runtime_error("Record does not satisfy conditions");
//         // }
//         return std::make_unique<RmRecord>(rec);
//     }



//     bool is_end() const override { return scan_->is_end(); }

//     std::string getType() { return "SeqScanExecutor"; };

//     Rid &rid() override { return rid_; }

//     static int compare(const Value &a, const Value &b) {
//         switch (a.type) {
//             case TYPE_INT:
//                 if (a.int_val > b.int_val)
//                     return 1;
//                 else if (a.int_val == b.int_val)
//                     return 0;
//                 else
//                     return -1;
//                 break;
//             case TYPE_FLOAT:
//                 if (a.float_val > b.float_val)
//                     return 1;
//                 else if (a.float_val == b.float_val)
//                     return 0;
//                 else
//                     return -1;
//                 break;
//             case TYPE_STRING:
//                 if (a.str_val > b.str_val)
//                     return 1;
//                 else if (a.str_val == b.str_val)
//                     return 0;
//                 else
//                     return -1;
//                 break;
//         }
//     }

//     void check_runtime_conds() {
//         for (auto &cond : fed_conds_) {
//             assert(cond.lhs_col.tab_name == tab_name_);
//             if (!cond.is_rhs_val) {
//                 assert(cond.rhs_col.tab_name == tab_name_);
//             }
//         }
//     }

//     bool eval_cond(const std::vector<ColMeta> &rec_cols, const Condition &cond, const RmRecord *rec) {
//         auto lhs_col = get_col(rec_cols, cond.lhs_col);
//         char *lhs = rec->data + lhs_col->offset;
//         char *rhs;
//         ColType rhs_type;
//         if (cond.is_rhs_val) {
//             rhs_type = cond.rhs_val.type;
//             rhs = cond.rhs_val.raw->data;
//         } else {
//             // rhs is a column
//             auto rhs_col = get_col(rec_cols, cond.rhs_col);
//             rhs_type = rhs_col->type;
//             rhs = rec->data + rhs_col->offset;
//         }
//         assert(rhs_type == lhs_col->type);  // TODO convert to common type
//         int cmp = ix_compare(lhs, rhs, rhs_type, lhs_col->len);
//         if (cond.op == OP_EQ) {
//             return cmp == 0;
//         } else if (cond.op == OP_NE) {
//             return cmp != 0;
//         } else if (cond.op == OP_LT) {
//             return cmp < 0;
//         } else if (cond.op == OP_GT) {
//             return cmp > 0;
//         } else if (cond.op == OP_LE) {
//             return cmp <= 0;
//         } else if (cond.op == OP_GE) {
//             return cmp >= 0;
//         } else {
//             throw InternalError("Unexpected op type");
//         }
//     }

//     bool eval_conds(const std::vector<ColMeta> &rec_cols, const std::vector<Condition> &conds, const RmRecord *rec) {
//         return std::all_of(conds.begin(), conds.end(),
//                            [&](const Condition &cond) { return eval_cond(rec_cols, cond, rec); });
//     }
// };
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