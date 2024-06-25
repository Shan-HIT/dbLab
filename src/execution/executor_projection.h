// // // /* Copyright (c) 2023 Renmin University of China
// // // RMDB is licensed under Mulan PSL v2.
// // // You can use this software according to the terms and conditions of the Mulan PSL v2.
// // // You may obtain a copy of Mulan PSL v2 at:
// // //         http://license.coscl.org.cn/MulanPSL2
// // // THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// // // EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// // // MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// // // See the Mulan PSL v2 for more details. */

// // // #pragma once
// // // #include "execution_defs.h"
// // // #include "execution_manager.h"
// // // #include "executor_abstract.h"
// // // #include "index/ix.h"
// // // #include "system/sm.h"

// // // class ProjectionExecutor : public AbstractExecutor {
// // //    private:
// // //     std::unique_ptr<AbstractExecutor> prev_;        // 投影节点的儿子节点
// // //     std::vector<ColMeta> cols_;                     // 需要投影的字段
// // //     size_t len_;                                    // 字段总长度
// // //     std::vector<size_t> sel_idxs_;                  

// // //    public:
// // //     ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) {
// // //         prev_ = std::move(prev);

// // //         size_t curr_offset = 0;
// // //         auto &prev_cols = prev_->cols();
// // //         for (auto &sel_col : sel_cols) {
// // //             auto pos = get_col(prev_cols, sel_col);
// // //             sel_idxs_.push_back(pos - prev_cols.begin());
// // //             auto col = *pos;
// // //             col.offset = curr_offset;
// // //             curr_offset += col.len;
// // //             cols_.push_back(col);
// // //         }
// // //         len_ = curr_offset;
// // //     }

// // //     void beginTuple() override {
// // //         prev_->beginTuple(); // 先向儿子节点请求元组
// // //         std::vector<ColMeta> prev_cols = prev_->cols();
// // //         std::cout<<"@@@@@@@@@@@@@@@@@@@@@@@@"<<std::endl;
// // //         std::cout<<prev_.get()<<"beginTuple"<<std::endl;
// // //         for (auto &col : prev_cols) {
// // //             col.offset = 0;
// // //         }
// // //     }

// // //     void nextTuple() override {
// // //         assert(!prev_->is_end());
// // //         prev_->nextTuple();
// // //     }
// // //     bool is_end() const override { return prev_->is_end(); }

// // // // std::unique_ptr<RmRecord> Next() override {
// // // //     assert(!is_end());
// // // //     auto &prev_cols = prev_->cols();
// // // //     auto prev_rec = prev_->Next();
// // // //     auto &proj_cols = cols_;
// // // //     auto proj_rec = std::make_unique<RmRecord>(len_);
// // // //     for (size_t proj_idx = 0; proj_idx < proj_cols.size(); proj_idx++) {
// // // //         size_t prev_idx = sel_idxs_[proj_idx];
// // // //         auto &prev_col = prev_cols[prev_idx];
// // // //         auto &proj_col = proj_cols[proj_idx];
// // // //         memcpy(proj_rec.get() + proj_col.offset, prev_rec.get() + prev_col.offset, prev_col.len);
// // // //     }
// // // //     return proj_rec;
// // // // }
// // //     // std::unique_ptr<RmRecord> Next() override {
// // //     //     auto Tuple = prev_->Next();
// // //     //     std::vector<std::string> columns;
// // //     //     std::string col_str;
// // //     //     auto old_cols = prev_->cols();
// // //     //     for (size_t i = 0; i < cols_.size(); i ++ ) {
// // //     //         auto old_col = old_cols[sel_idxs_[i]];
// // //     //         char *rec_buf = Tuple->data + old_col.offset;
// // //     //         col_str += std::string(rec_buf, old_col.len);
// // //     //     }

// // //     //     RmRecord rec(len_);
// // //     //     rec.SetData(col_str.c_str());

// // //     //     return std::make_unique<RmRecord>(rec);
// // //     // }
// // // std::unique_ptr<RmRecord> Next() override {
// // //     auto Tuple = prev_->Next();
// // //     std::vector<std::string> columns;
// // //     std::string col_str;
// // //     auto old_cols = prev_->cols();
// // //     for (size_t i = 0; i < cols_.size(); i ++ ) {
// // //         auto old_col = old_cols[sel_idxs_[i]];
// // //         char *rec_buf = Tuple->data + old_col.offset;
// // //         col_str += std::string(rec_buf, old_col.len);
// // //     }
// // //     RmRecord rec(len_);
// // //     char* modifiable_str = new char[col_str.length() + 1];
// // //     std::strcpy(modifiable_str, col_str.c_str());
// // //     rec.SetData(modifiable_str);
// // //     return std::make_unique<RmRecord>(rec);
// // // }


// // //     Rid &rid() override { return _abstract_rid; }
// // // };
// // /* Copyright (c) 2023 Renmin University of China
// // RMDB is licensed under Mulan PSL v2.
// // You can use this software according to the terms and conditions of the Mulan PSL v2.
// // You may obtain a copy of Mulan PSL v2 at:
// //         http://license.coscl.org.cn/MulanPSL2
// // THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// // EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// // MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// // See the Mulan PSL v2 for more details. */

// // #pragma once
// // #include "execution_defs.h"
// // #include "execution_manager.h"
// // #include "executor_abstract.h"
// // #include "index/ix.h"
// // #include "system/sm.h"

// // class ProjectionExecutor : public AbstractExecutor {
// //    private:
// //     std::unique_ptr<AbstractExecutor> prev_;        // 投影节点的儿子节点
// //     std::vector<ColMeta> cols_;                     // 需要投影的字段
// //     size_t len_;                                    // 字段总长度
// //     std::vector<size_t> sel_idxs_;                  // 需要投影的字段在原表中的位置
// //     std::vector<RmRecord> recs;    // 用于去重

// //    public:
// //     ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) {
// //         prev_ = std::move(prev);

// //         size_t curr_offset = 0;
// //         auto &prev_cols = prev_->cols();
// //         for (auto &sel_col : sel_cols) {
// //             auto pos = get_col(prev_cols, sel_col);
// //             sel_idxs_.push_back(pos - prev_cols.begin());
// //             auto col = *pos;
// //             col.offset = curr_offset;
// //             curr_offset += col.len;
// //             cols_.push_back(col);
// //         }
// //         len_ = curr_offset;
// //     }

// //     const std::vector<ColMeta> &cols() const override {
// //         return cols_;
// //     }

// //     size_t tupleLen() const override {
// //         return len_;
// //     }

// //     std::string getType() override {
// //         return "ProjectionExecutor";
// //     }

// //     void beginTuple() override {
// //         std::cout<<"Begin可以进来"<<std::endl;
// //         prev_->beginTuple();
// //         std::cout<<"Begin可以结束"<<std::endl;
// //     }

// //     void nextTuple() override {
// //         std::cout<<"Next可以进来"<<std::endl;
// //         auto rec = prev_->Next();
// //         auto new_rec = std::make_unique<RmRecord>(len_);
// //         intoNewRec(rec, new_rec);
// //         recs.push_back(*new_rec);
// //         std::cout<<"Next可以结束"<<std::endl;
// //     }

// //     // bool is_end() const override {
// //     //     std::cout<<"is_end可以进来"<<std::endl;
// //     //     while (true) {
// //     //         std::cout<<"is_end循环"<<std::endl;
// //     //         auto rec = prev_->Next();
// //     //         std::cout<<"is_end循环结束"<<std::endl;
// //     //         if (prev_->is_end() || rec == nullptr) {
// //     //             return true;
// //     //         }
// //     //         std::cout<<"is_end循环2"<<std::endl;
// //     //         auto new_rec = std::make_unique<RmRecord>(len_);
// //     //         intoNewRec(rec, new_rec);
// //     //         if (isNotDuplicate(new_rec)){
// //     //             return false;
// //     //         }
// //     //         prev_->nextTuple();
// //     //         std::cout<<"is_end循环3"<<std::endl;
// //     //     }
// //     //     std::cout<<"is_end可以结束"<<std::endl;
// //     // }
// //     bool is_end() const override {
// //         std::cout<<"is_end可以进来"<<std::endl;
// //         while (true) {
// //             std::cout<<"is_end循环"<<std::endl;
// //             try {
// //                 auto rec = prev_->Next();
// //                 std::cout<<"is_end循环结束"<<std::endl;
// //                 if (prev_->is_end() || rec == nullptr) {
// //                     return true;
// //                 }
// //                 std::cout<<"is_end循环2"<<std::endl;
// //                 auto new_rec = std::make_unique<RmRecord>(len_);
// //                 intoNewRec(rec, new_rec);
// //                 if (isNotDuplicate(new_rec)){
// //                     return false;
// //                 }
// //                 prev_->nextTuple();
// //                 std::cout<<"is_end循环3"<<std::endl;
// //             } catch (const std::runtime_error& e) {
// //                 std::cout << "Caught a runtime_error exception: " << e.what() << '\n';
// //                 return true;
// //             }
// //         }
// //         std::cout<<"is_end可以结束"<<std::endl;
// //     }

// //     bool isNotDuplicate(std::unique_ptr<RmRecord> &rec) const {
// //         for (auto &r : recs) {
// //             if (memcmp(r.data, rec->data, len_) == 0) {
// //                 return false;
// //             }
// //         }
// //         return true;
// //     }

// //     void intoNewRec(std::unique_ptr<RmRecord> &rec, std::unique_ptr<RmRecord> &new_rec) const {
// //         for (size_t i = 0; i < sel_idxs_.size(); i++) {
// //             auto &col = cols_[i];
// //             auto &prev_col = prev_->cols()[sel_idxs_[i]];
// //             auto rec_buf = rec->data + prev_col.offset;
// //             auto new_rec_buf = new_rec->data + col.offset;
// //             if (col.type == TYPE_INT) {
// //                 *(int *)new_rec_buf = *(int *)rec_buf;
// //             } else if (col.type == TYPE_FLOAT) {
// //                 *(float *)new_rec_buf = *(float *)rec_buf;
// //             } else if (col.type == TYPE_STRING) {
// //                 memcpy(new_rec_buf, rec_buf, col.len);
// //             }
// //         }
// //     }

// //     // std::unique_ptr<RmRecord> Next() override {
// //     //     std::cout<<"Next可以进来"<<std::endl;
// //     //     auto rec = prev_->Next();
// //     //     auto new_rec = std::make_unique<RmRecord>(len_);
// //     //     intoNewRec(rec, new_rec);
// //     //     recs.push_back(*new_rec);
// //     //     return new_rec;
// //     // }
// //     std::unique_ptr<RmRecord> Next() override {
// //         std::cout<<"Next可以进来"<<std::endl;
// //         if (!is_end()) {
// //             auto rec = prev_->Next();
// //             auto new_rec = std::make_unique<RmRecord>(len_);
// //             intoNewRec(rec, new_rec);
// //             recs.push_back(*new_rec);
// //             return new_rec;
// //         } else {
// //             std::cout << "No more records to fetch. Returning nullptr." << std::endl;
// //             return nullptr;
// //         }
// //     }

// //     Rid &rid() override { return _abstract_rid; }
// // };

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

// class ProjectionExecutor : public AbstractExecutor {
//    private:
//     std::unique_ptr<AbstractExecutor> prev_;        // 投影节点的儿子节点
//     std::vector<ColMeta> cols_;                     // 需要投影的字段
//     size_t len_;                                    // 字段总长度
//     std::vector<size_t> sel_idxs_;                  // 需要投影的字段在原表中的位置
//     std::vector<RmRecord> recs;    // 用于去重

//    public:
//     ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) {
//         prev_ = std::move(prev);

//         size_t curr_offset = 0;
//         auto &prev_cols = prev_->cols();
//         for (auto &sel_col : sel_cols) {
//             auto pos = get_col(prev_cols, sel_col);
//             sel_idxs_.push_back(pos - prev_cols.begin());
//             auto col = *pos;
//             col.offset = curr_offset;
//             curr_offset += col.len;
//             cols_.push_back(col);
//         }
//         len_ = curr_offset;
//     }

//     const std::vector<ColMeta> &cols() const override {
//         return cols_;
//     }

//     size_t tupleLen() const override {
//         return len_;
//     }

//     std::string getType() override {
//         return "ProjectionExecutor";
//     }

//     void beginTuple() override {
//         prev_->beginTuple();
//     }

//     void nextTuple() override {
//         auto rec = prev_->Next();
//         auto new_rec = std::make_unique<RmRecord>(len_);
//         intoNewRec(rec, new_rec);
//         recs.push_back(*new_rec);
//     }

//     bool is_end() const override {
//         while (true) {
//             auto rec = prev_->Next();
//             if (prev_->is_end() || rec == nullptr) {
//                 return true;
//             }
//             auto new_rec = std::make_unique<RmRecord>(len_);
//             intoNewRec(rec, new_rec);
//             if (isNotDuplicate(new_rec)){
//                 return false;
//             }
//             prev_->nextTuple();
//         }
//     }

//     bool isNotDuplicate(std::unique_ptr<RmRecord> &rec) const {
//         for (auto &r : recs) {
//             if (memcmp(r.data, rec->data, len_) == 0) {
//                 return false;
//             }
//         }
//         return true;
//     }

//     void intoNewRec(std::unique_ptr<RmRecord> &rec, std::unique_ptr<RmRecord> &new_rec) const {
//         for (size_t i = 0; i < sel_idxs_.size(); i++) {
//             auto &col = cols_[i];
//             auto &prev_col = prev_->cols()[sel_idxs_[i]];
//             auto rec_buf = rec->data + prev_col.offset;
//             auto new_rec_buf = new_rec->data + col.offset;
//             if (col.type == TYPE_INT) {
//                 *(int *)new_rec_buf = *(int *)rec_buf;
//             } else if (col.type == TYPE_FLOAT) {
//                 *(float *)new_rec_buf = *(float *)rec_buf;
//             } else if (col.type == TYPE_STRING) {
//                 memcpy(new_rec_buf, rec_buf, col.len);
//             }
//         }
//     }

//     std::unique_ptr<RmRecord> Next() override {
//         auto rec = prev_->Next();
//         auto new_rec = std::make_unique<RmRecord>(len_);
//         intoNewRec(rec, new_rec);
//         recs.push_back(*new_rec);
//         return new_rec;
//     }

//     Rid &rid() override { return _abstract_rid; }
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