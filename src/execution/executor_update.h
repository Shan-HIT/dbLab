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

// class UpdateExecutor : public AbstractExecutor {
//    private:
//     TabMeta tab_;
//     std::vector<Condition> conds_;
//     RmFileHandle *fh_;
//     std::vector<Rid> rids_;
//     std::string tab_name_;
//     std::vector<SetClause> set_clauses_;
//     SmManager *sm_manager_;

//    public:
//     UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
//                    std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
//         sm_manager_ = sm_manager;
//         tab_name_ = tab_name;
//         set_clauses_ = set_clauses;
//         tab_ = sm_manager_->db_.get_table(tab_name);
//         fh_ = sm_manager_->fhs_.at(tab_name).get();
//         conds_ = conds;
//         rids_ = rids;
//         context_ = context;
//     }
//     bool isSatisfy (Rid rid) {
//         auto rec = fh_->get_record(rid, context_); // 获取记录
//         for (auto &cond : conds_) { // 遍历所有的条件
//             if (cond.is_rhs_val) { // 如果右侧是值
//                 for (auto &col : cols_) { // 遍历所有的字段
//                     if (col.name == cond.lhs_col.col_name) { // 找到左侧的字段
//                         Value lhs = toValue(*rec, col); // 获取左侧的值
//                         if (!compareBtw(lhs, cond.rhs_val, cond.op)) { // 比较左右两侧的值
//                             return false;
//                         }
//                     }
//                 }
//             } else {
//                 Value lhs, rhs;
//                 for (auto &col : cols_) {
//                     if (col.name == cond.lhs_col.col_name) {
//                         lhs = toValue(*rec, col);
//                     }
//                     if (col.name == cond.rhs_col.col_name) {
//                         rhs = toValue(*rec, col);
//                     }
//                 }
//                 if (!compareBtw(lhs, rhs, cond.op)) {
//                     return false;
//                 }
//             }
//         }
//         return true;
//     }

//     Value toValue(const RmRecord &rec, ColMeta &col) {
//         Value val;
//         switch (col.type) {
//             case TYPE_INT:
//                 val.type = TYPE_INT;
//                 val.int_val = *reinterpret_cast<int *>(rec.data + col.offset);
//                 break;
//             case TYPE_FLOAT:
//                 val.type = TYPE_FLOAT;
//                 val.float_val = *reinterpret_cast<float *>(rec.data + col.offset);
//                 break;
//             case TYPE_STRING:
//                 val.type = TYPE_STRING;
//                 val.str_val = std::string(rec.data + col.offset, col.len);
//                 break;
//             default:
//                 assert(false);
//         }
//         return val;
//     }

//     bool compareBtw(const Value &lhs, const Value &rhs, CompOp op) {
//         switch (lhs.type) {
//             case TYPE_INT:
//                 switch (op) {
//                     case OP_EQ:
//                         return lhs.int_val == rhs.int_val;
//                     case OP_LT:
//                         return lhs.int_val < rhs.int_val;
//                     case OP_GT:
//                         return lhs.int_val > rhs.int_val;
//                     case OP_LE:
//                         return lhs.int_val <= rhs.int_val;
//                     case OP_GE:
//                         return lhs.int_val >= rhs.int_val;
//                     case OP_NE:
//                         return lhs.int_val != rhs.int_val;
//                     default:
//                         assert(false);
//                 }
//             case TYPE_FLOAT:
//                 switch (op) {
//                     case OP_EQ:
//                         return lhs.float_val == rhs.float_val;
//                     case OP_LT:
//                         return lhs.float_val < rhs.float_val;
//                     case OP_GT:
//                         return lhs.float_val > rhs.float_val;
//                     case OP_LE:
//                         return lhs.float_val <= rhs.float_val;
//                     case OP_GE:
//                         return lhs.float_val >= rhs.float_val;
//                     case OP_NE:
//                         return lhs.float_val != rhs.float_val;
//                     default:
//                         assert(false);
//                 }
//             case TYPE_STRING:
//                 switch (op) {
//                     case OP_EQ:
//                         return lhs.str_val == rhs.str_val;
//                     case OP_LT:
//                         return lhs.str_val < rhs.str_val;
//                     case OP_GT:
//                         return lhs.str_val > rhs.str_val;
//                     case OP_LE:
//                         return lhs.str_val <= rhs.str_val;
//                     case OP_GE:
//                         return lhs.str_val >= rhs.str_val;
//                     case OP_NE:
//                         return lhs.str_val != rhs.str_val;
//                     default:
//                         assert(false);
//                 }
//             default:
//                 assert(false);
//         }
//     }
//     std::unique_ptr<RmRecord> Next() override {
//         if (rids_.empty()) {
//             return nullptr;
//         }

//         Rid rid = rids_.back();
//         rids_.pop_back();

//         RmRecord rec(fh_->get_file_hdr().record_size);
//         // fh_->get_rec(rid, rec);

//         for (const auto &set_clause : set_clauses_) {
//             if (set_clause.cond.evaluate(rec)) {
//                 set_clause.apply(rec);
//             }
//         }

//         fh_->update_rec(rid, rec);

//         return std::make_unique<RmRecord>(std::move(rec));
//     }


//     Rid &rid() override { return _abstract_rid; }
// };
#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    bool isSatisfy (Rid rid) {
        auto rec = fh_->get_record(rid, context_); // 获取记录
        auto cols_ = tab_.cols;
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
    std::unique_ptr<RmRecord> Next() override {
        for(auto &rid : rids_){
            if(isSatisfy(rid)){
                auto rec = fh_->get_record(rid, context_); // 获取记录
                for(auto &set_clauses : set_clauses_){
                    for(auto &col : tab_.cols){
                        if(col.name == set_clauses.lhs.col_name){
                            Value val = set_clauses.rhs;
                            if(col.type != val.type){
                                throw IncompatibleTypeError(coltype2str(col.type),coltype2str(val.type));
                            }
                            memcpy(rec->data + col.offset, val.raw->data, col.len);
                        }
                    }
                }
                fh_->update_record(rid,rec->data,context_);
            }
        }
        return nullptr;
    }
    Rid &rid() override { return _abstract_rid; }
};
