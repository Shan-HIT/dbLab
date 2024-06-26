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

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend;                                 // 是否结束

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, 
                            std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);

    }

    const std::vector<ColMeta> &cols() const override {
        return cols_;
    };

    size_t tupleLen() const override {
        return len_;
    }

    std::string getType() override {
        return "NestedLoopJoinExecutor";
    }

    // bool isConditionSatisfied (std::unique_ptr<RmRecord> lrec, std::unique_ptr<RmRecord> rrec) {
    //     for (auto &cond : fed_conds_) { // 遍历所有的条件
    //         if (cond.is_rhs_val) { // 如果右侧是值
    //             for (auto &col : cols_) { // 遍历所有的字段
    //                 if (col.name == cond.lhs_col.col_name) { // 找到左侧的字段
    //                     Value lhs = toValue(*lrec, col); // 获取左侧的值
    //                     if (!eval_conds(lhs, cond.rhs_val, cond.op)) { // 比较左右两侧的值
    //                         return false;
    //                     }
    //                 }
    //             }
    //         } else { // 如果右侧是字段
    //             Value lhs, rhs;
    //             std::vector<ColMeta> left_cols = left_->cols();
    //             std::vector<ColMeta> right_cols = right_->cols();
    //             for (auto &col : left_cols) {
    //                 if (col.name == cond.lhs_col.col_name) {
    //                     lhs = toValue(*lrec, col);
    //                 }
    //             }
    //             for (auto &col : right_cols) {
    //                 if (col.name == cond.rhs_col.col_name) {
    //                     rhs = toValue(*rrec, col);
    //                 }
    //             }
    //             if (!eval_conds(lhs, rhs, cond.op)) {
    //                 return false;
    //             }
    //         }
    //     }
    //     return true;
    // }

    // Value toValue(const RmRecord &rec, ColMeta &col) {
    //     Value val;
    //     switch (col.type) {
    //         case TYPE_INT:
    //             val.type = TYPE_INT;
    //             val.int_val = *reinterpret_cast<int *>(rec.data + col.offset);
    //             break;
    //         case TYPE_FLOAT:
    //             val.type = TYPE_FLOAT;
    //             val.float_val = *reinterpret_cast<float *>(rec.data + col.offset);
    //             break;
    //         case TYPE_STRING:
    //             val.type = TYPE_STRING;
    //             val.str_val = std::string(rec.data + col.offset, col.len);
    //             break;
    //         default:
    //             assert(false);
    //     }
    //     return val;
    // }
    bool isConditionSatisfied (std::unique_ptr<RmRecord> lrec, std::unique_ptr<RmRecord> rrec) {
        for (auto &cond : fed_conds_) { // 遍历所有的条件
            if (cond.is_rhs_val) { // 如果右侧是值
                for (auto &col : cols_) { // 遍历所有的字段
                    if (col.name == cond.lhs_col.col_name) { // 找到左侧的字段
                        Value lhs;
                        switch (col.type) {
                            case TYPE_INT:
                                lhs.type = TYPE_INT;
                                lhs.int_val = *reinterpret_cast<int *>(lrec->data + col.offset);
                                break;
                            case TYPE_FLOAT:
                                lhs.type = TYPE_FLOAT;
                                lhs.float_val = *reinterpret_cast<float *>(lrec->data + col.offset);
                                break;
                            case TYPE_STRING:
                                lhs.type = TYPE_STRING;
                                lhs.str_val = std::string(lrec->data + col.offset, col.len);
                                break;
                            default:
                                assert(false);
                        }
                        if (!eval_conds(lhs, cond.rhs_val, cond.op)) { // 比较左右两侧的值
                            return false;
                        }
                    }
                }
            } else { // 如果右侧是字段
                Value lhs, rhs;
                std::vector<ColMeta> left_cols = left_->cols();
                std::vector<ColMeta> right_cols = right_->cols();
                for (auto &col : left_cols) {
                    if (col.name == cond.lhs_col.col_name) {
                        switch (col.type) {
                            case TYPE_INT:
                                lhs.type = TYPE_INT;
                                lhs.int_val = *reinterpret_cast<int *>(lrec->data + col.offset);
                                break;
                            case TYPE_FLOAT:
                                lhs.type = TYPE_FLOAT;
                                lhs.float_val = *reinterpret_cast<float *>(lrec->data + col.offset);
                                break;
                            case TYPE_STRING:
                                lhs.type = TYPE_STRING;
                                lhs.str_val = std::string(lrec->data + col.offset, col.len);
                                break;
                            default:
                                assert(false);
                        }
                    }
                }
                for (auto &col : right_cols) {
                    if (col.name == cond.rhs_col.col_name) {
                        switch (col.type) {
                            case TYPE_INT:
                                rhs.type = TYPE_INT;
                                rhs.int_val = *reinterpret_cast<int *>(rrec->data + col.offset);
                                break;
                            case TYPE_FLOAT:
                                rhs.type = TYPE_FLOAT;
                                rhs.float_val = *reinterpret_cast<float *>(rrec->data + col.offset);
                                break;
                            case TYPE_STRING:
                                rhs.type = TYPE_STRING;
                                rhs.str_val = std::string(rrec->data + col.offset, col.len);
                                break;
                            default:
                                assert(false);
                        }
                    }
                }
                if (!eval_conds(lhs, rhs, cond.op)) {
                    return false;
                }
            }
        }
        return true;
    }


    bool eval_conds(const Value &lhs, const Value &rhs, CompOp op) {
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

    void beginTuple() override {
        left_->beginTuple();
        right_->beginTuple();
    }

    void nextTuple() override {
        if (isend) {
            return;
        }
        while (true) { // 一直循环直到找到满足条件的元组
            if (right_->is_end()) { 
                if (left_->is_end()) { // 如果左右两侧都结束了
                    isend = true;
                    return;
                }
                left_->nextTuple();
                right_->beginTuple();
            } else {
                right_->nextTuple();
            }
            if (isConditionSatisfied(left_->Next(), right_->Next())) { // 如果满足条件
                return;
            }
        }
    }

    bool is_end() const override {
        return isend;
    }

    void makeConnect(RmRecord &rec, RmRecord &left_rec, RmRecord &right_rec) {
        memcpy(rec.data, left_rec.data, left_->tupleLen());
        memcpy(rec.data + left_->tupleLen(), right_rec.data, right_->tupleLen());
    }

    std::unique_ptr<RmRecord> Next() override {
        auto left_rec = left_->Next();
        auto right_rec = right_->Next();
        if (left_rec == nullptr || right_rec == nullptr) {
            return nullptr;
        }
        auto rec = std::make_unique<RmRecord>(len_);
        makeConnect(*rec, *left_rec, *right_rec);
        return rec;
    }

    Rid &rid() override { return _abstract_rid; }
};