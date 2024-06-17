by Moonastger

`lower_bound`函数：
```C++
int IxNodeHandle::lower_bound(const char *target) const {
    // Todo:
    // 查找当前节点中第一个大于等于target的key，并返回key的位置给上层
    // 提示: 可以采用多种查找方式，如顺序遍历、二分查找等；使用ix_compare()函数进行比较
    int key_idx = 0;
    int end = page_hdr->num_key;
    while(key_idx < end){
        int mid = (key_idx + end) >> 1;
        if (ix_compare(get_key(mid), target, file_hdr->col_types_, file_hdr->col_lens_) < 0) 
            key_idx = mid + 1;
        else
            end = mid;
    }
    return key_idx;
}

```
一个很简单的二分，值得注意的是它的返回值是小于等于目标值的第一个元素。
`upper_bound`函数：
```C++
int IxNodeHandle::upper_bound(const char *target) const {
    // Todo:
    // 查找当前节点中第一个大于target的key，并返回key的位置给上层
    // 提示: 可以采用多种查找方式：顺序遍历、二分查找等；使用ix_compare()函数进行比较
    int key_idx = 1;
    int end = page_hdr->num_key;
    while (key_idx < end)
    {
        int mid = (key_idx + end) >> 1;
        if(ix_compare(get_key(mid), target, file_hdr->col_types_, file_hdr->col_lens_) <= 0)
            key_idx = mid + 1;
        else
            end = mid;
    }
    return key_idx;
}
```
也是一个很简单的二分，值得注意的是它的返回值是大于目标值的第一个元素。
`leaf_lookup`函数：
```C++
bool IxNodeHandle::leaf_lookup(const char *key, Rid **value) {
    // Todo:
    // 1. 在叶子节点中获取目标key所在位置
    // 2. 判断目标key是否存在
    // 3. 如果存在，获取key对应的Rid，并赋值给传出参数value
    // 提示：可以调用lower_bound()和get_rid()函数。
    int position = lower_bound(key);
    if (position != page_hdr->num_key && ix_compare(get_key(position), key, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
        *value = get_rid(position);
        return true;
    }
    return false;
}
```
这个函数的功能是在叶子节点链表中寻找目标`key`的位置，并判断是否存在，如果存在，则返回其对应的`Rid`。
实现思路很简单，直接用链表的二分查找，然后比较，找到就是找到，没找到就是没找到
`internal_lookup`函数：
```C++
page_id_t IxNodeHandle::internal_lookup(const char *key) {
    // Todo:
    // 1. 查找当前非叶子节点中目标key所在孩子节点（子树）的位置
    // 2. 获取该孩子节点（子树）所在页面的编号
    // 3. 返回页面编号
    int position = upper_bound(key);
    if(position)
        return value_at(position - 1);
    return value_at(position);
}
```
这个函数是在内节点中查找目标键所在的子树的位置

`insert_pairs`函数：
```C++
/**
 * @brief 在指定位置插入n个连续的键值对
 * 将key的前n位插入到原来keys中的pos位置；将rid的前n位插入到原来rids中的pos位置
 *
 * @param pos 要插入键值对的位置
 * @param (key, rid) 连续键值对的起始地址，也就是第一个键值对，可以通过(key, rid)来获取n个键值对
 * @param n 键值对数量
 * @note [0,pos)           [pos,num_key)
 *                            key_slot
 *                            /      \
 *                           /        \
 *       [0,pos)     [pos,pos+n)   [pos+n,num_key+n)
 *                      key           key_slot
 */
void IxNodeHandle::insert_pairs(int pos, const char *key, const Rid *rid, int n) {
    // Todo:
    // 1. 判断pos的合法性
    // 2. 通过key获取n个连续键值对的key值，并把n个key值插入到pos位置
    // 3. 通过rid获取n个连续键值对的rid值，并把n个rid值插入到pos位置
    // 4. 更新当前节点的键数量
    assert(pos >= 0 && pos <= page_hdr->num_key);

    auto tot_len = file_hdr->col_tot_len_;
    auto key_len = tot_len * n;
    auto last_key_len = tot_len * (page_hdr->num_key - pos);
    auto rid_len = sizeof(Rid) * n;
    auto last_rid_len =  sizeof(Rid) * (page_hdr->num_key - pos);

    memmove(get_key(pos + n), get_key(pos), last_key_len);
    memmove(get_rid(pos + n), get_rid(pos), last_rid_len);

    auto now_keys = get_key(pos);
    auto now_rids = get_rid(pos);
    memcpy(now_keys, key, key_len);
    memcpy(now_rids, rid, rid_len);

    page_hdr->num_key += n;

}
```
这个函数的作用是将`n`个连续的键值对插入到指定位置，并更新当前节点的键数量。一般普通的单个插入就是把它的最后一个参数置1即可，多项插入一般用于分裂操作
它的实现是这样的，首先使用断言判断非法情况，之后进行计算键和Rid的长度：
    `tot_len`表示每个键值对的总长度
    `key_len`表示n个键值对的总长度
    `last_key_len`表示从`pos`位置开始到结尾的键的长度
    `rid_len`表示`n`个`Rid`的总长度
    `last_rid_len`表示从`pos`位置到结尾的Rid的长度
计算这些值的必要性是为了后面的内存移动
键值对指的就是`{key,Rid}`，这里的`Rid`代替了普遍意义上的“值”的作用
然后使用`memmove`函数将原来位置的键值对向后移动，并将新的键值对插入到指定位置
最后更新当前节点的键数量
    `auto now_keys = get_key(pos);`
    `auto now_rids = get_rid(pos);`
这两行代码的作用是获取当前位置的键和`Rid`的起始地址，方便后续的内存拷贝

`insert`函数
```C++
int IxNodeHandle::insert(const char *key, const Rid &value) {
    // Todo:
    // 1. 查找要插入的键值对应该插入到当前节点的哪个位置
    // 2. 如果key重复则不插入
    // 3. 如果key不重复则插入键值对
    // 4. 返回完成插入操作之后的键值对数量
    int position = lower_bound(key);
    if (!page_hdr->num_key || ix_compare(get_key(position), key, file_hdr->col_types_, file_hdr->col_lens_) != 0) { 
        insert_pairs(position, key, &value, 1);
    }
    return get_size();
}
```
他这个函数实际上就是绝大多数情况下Insert_pairs函数中插入数量为1的情况，但是它还有一个判断是否重复的操作，如果重复了就不插入，如果不重复就插入。
`!page_hdr->num_key || ix_compare(get_key(position), key, file_hdr->col_types_, file_hdr->col_lens_) != 0`它的意思是如果当前节点为空或者当前节点的第一个键值对的key值大于要插入的key值，那么就插入，否则不插入。

`erase_pair`函数：
```C++
void IxNodeHandle::erase_pair(int pos) {
    // Todo:
    // 1. 删除该位置的key
    // 2. 删除该位置的rid
    // 3. 更新结点的键值对数量
    auto next_len = page_hdr->num_key - pos - 1;//从pos位置开始，直到该结点末尾的剩余的key的数量
    memmove(get_key(pos), get_key(pos + 1), next_len * file_hdr->col_tot_len_);
    memmove(get_rid(pos), get_rid(pos + 1), next_len * sizeof(Rid));
    page_hdr->num_key--;
    return;
}
```
与插入相对的删除，不过它在实现的时候是从单个删除实现的，并不是多个删除。思路也是内存移动
`remove`函数：
```C++
int IxNodeHandle::remove(const char *key) {
    // Todo:
    // 1. 查找要删除键值对的位置
    // 2. 如果要删除的键值对存在，删除键值对
    // 3. 返回完成删除操作后的键值对数量
    int idx = lower_bound(key);
    if (idx != page_hdr->num_key && ix_compare(key, get_key(idx), file_hdr->col_types_, file_hdr->col_lens_) == 0){
        erase_pair(idx);
    }
    return page_hdr->num_key;
}
```
这个函数实际上是对`erase`函数的包装。`erase`是个非常原子的操作，只负责在内存层面删除一个键值对，而`remove`则是对其进行查找，并删除的操作。
值得注意的是,`page_hdr->num_key`是在`erase_pair`函数更新的，注意不要重复更新，这种小毛病说实话挺麻烦

`find_leaf_page`函数：
```C++
std::pair<IxNodeHandle *, bool> IxIndexHandle::find_leaf_page(const char *key, Operation operation,
                                                            Transaction *transaction, bool find_first) {
    // Todo:
    // 1. 获取根节点
    // 2. 从根节点开始不断向下查找目标key
    // 3. 找到包含该key值的叶子结点停止查找，并返回叶子节点
    auto root = fetch_node(file_hdr_->root_page_);
    auto target = root;
    if(operation == Operation::INSERT){
        while(!target->is_leaf_page()){
            auto parent_node = target;
            target = fetch_node(target->internal_lookup(key));
            buffer_pool_manager_->unpin_page(parent_node->get_page_id(), false);
        }
    }else if(operation == Operation::FIND){
        while(!target->is_leaf_page()){
            auto parent_node = target;
            target = fetch_node(target->internal_lookup(key));
            buffer_pool_manager_->unpin_page(parent_node->get_page_id(), false);
        }
    } else{
        while(!target->is_leaf_page()){
            auto parent_node = target;
            target = fetch_node(target->internal_lookup(key));
            buffer_pool_manager_->unpin_page(parent_node->get_page_id(), false);
        }
    }
    return std::make_pair(target, false);
}
```
这个函数的实现实际上是有争议的，争议点在于要不要区分这么多的`operation`。实际上可以发现，每一个条件分支下的代码是完全相同的
它的核心实现思路说实话很像一个链表，使用`fetch_node`先得到根节点，调用`internal_lookup`来获取其叶节点所在的子树，并用`fetch_node`得到这个节点(页面)。事实上，这也是循环体中的主要内容，不断取子树直到取到的节点是叶子节点，很像链表吧（笑）。
值得注意的是，每调用一个页面最后要及时`unpin`，否则这个页面会一直存在于缓冲池，性能下降都是小事，什么时候内存爆了就够你喝一壶的（笑x2）
至于它是什么时候被`pin`的呢？`fetch_node`

get_value函数：
```C++
bool IxIndexHandle::get_value(const char *key, std::vector<Rid> *result, Transaction *transaction) {
    // Todo:
    // 1. 获取目标key值所在的叶子结点
    // 2. 在叶子节点中查找目标key值的位置，并读取key对应的rid
    // 3. 把rid存入result参数中
    // 提示：使用完buffer_pool提供的page之后，记得unpin page；记得处理并发的上锁
    std::scoped_lock lock{root_latch_};
    auto leaf = find_leaf_page(key, Operation::FIND, transaction);
    Rid **rid = new Rid*();
    if(leaf.first->leaf_lookup(key, rid)){
        result->push_back(**rid);
        buffer_pool_manager_->unpin_page(leaf.first->get_page_id(), false);
        return true;
    }
    else{
        return false;
    }
}
```
上锁暂时不用管，可以使用固定的写法。不过这是个粗粒度的处理方法，<del style="color:yellow">细粒度的处理方法后续可能不会提到</del>
首先使用包装好的find_leaf_page函数找到目标`key`所在的叶子节点，返回的是一个树控制器类的指针。
然后调用leaf_lookup函数查找目标的叶子节点。注意,它的直接返回值是一个`std::pair<IxNodeHandle *, bool>`类型，第一项(即.first)可以看作是叶子节点的指针，第二项(即.second)表示是否找到了目标`key`。
如果找到了目标`key`，则把它的`Rid`存入`result`参数中，并`unpin`这个页面。注意一下这个是聚合索引，一个键会有多个字段，所有它使用`std::vector<Rid>*`类型来存返回值
如果没找到目标`key`，则返回`false`。

split函数
```C++
IxNodeHandle *IxIndexHandle::split(IxNodeHandle *node) {
    // Todo:
    // 1. 将原结点的键值对平均分配，右半部分分裂为新的右兄弟结点
    //    需要初始化新节点的page_hdr内容
    // 2. 如果新的右兄弟结点是叶子结点，更新新旧节点的prev_leaf和next_leaf指针
    //    为新节点分配键值对，更新旧节点的键值对数记录
    // 3. 如果新的右兄弟结点不是叶子结点，更新该结点的所有孩子结点的父节点信息(使用IxIndexHandle::maintain_child())
    IxNodeHandle *new_node = create_node();
    new_node->page_hdr->num_key = 0;
    new_node->page_hdr->next_free_page_no = node->page_hdr->next_free_page_no;
    new_node->set_parent_page_no(node->get_parent_page_no());
    new_node->page_hdr->is_leaf = node->page_hdr->is_leaf;

    int mid = node->page_hdr->num_key >> 1;
    new_node->insert_pairs(0, node->get_key(mid), node->get_rid(mid), node->page_hdr->num_key - mid);
    node->page_hdr->num_key = mid;

    if(node->page_hdr->is_leaf){
        IxNodeHandle *old_next_node = fetch_node(node->page_hdr->next_leaf);
        new_node->set_next_leaf(node->get_next_leaf());
        new_node->set_prev_leaf(node->get_page_no());
        node->set_next_leaf(new_node->get_page_no());
        old_next_node->set_prev_leaf(new_node->get_page_no());
        buffer_pool_manager_->unpin_page(old_next_node->get_page_id(), true);

        if(node->file_hdr->last_leaf_ == node->get_page_no()){
            file_hdr_->last_leaf_ = new_node->get_page_no();
        }

    } else{
        for(int i = 0; i < new_node->page_hdr->num_key; ++i){
            maintain_child(new_node, i);
        }
    }
    return new_node;
}

```
分裂函数。一旦出了bug会比较难受，建议是一次性写对
首先直接调用`create_node`函数创建一个全新的节点，接下来就是冗长的初始化过程。有人会使用一个自定义的init的函数来初始化，我替你们试过了，不建议这么做。
分裂节点的方式是从中间开始分裂，所有我们要找中点，并向新节点插入。注意这里插入的是原节点的右半部分，并不是左半部分。
之后需要分情况讨论这个分裂的节点是内部节点还是叶子节点，叶子节点的情况比较简单，也就是类似的初始化过程，不过要更新`node->file_hdr->last_leaf_ `
而内节点要考虑维护叶子节点，考虑遍历调用`maintain_child`<del>这个黑盒</del>函数，显然成立
显然成立，很神奇吧

`insert_into_parent`函数：
```C++
void IxIndexHandle::insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node,
                                     Transaction *transaction) {
    // Todo:
    // 1. 分裂前的结点（原结点, old_node）是否为根结点，如果为根结点需要分配新的root
    // 2. 获取原结点（old_node）的父亲结点
    // 3. 获取key对应的rid，并将(key, rid)插入到父亲结点
    // 4. 如果父亲结点仍需要继续分裂，则进行递归插入
    // 提示：记得unpin page
    if(old_node->is_root_page()){
        IxNodeHandle *new_root = create_node();
        new_root->page_hdr->is_leaf = false;
        new_root->page_hdr->num_key = 0;
        new_root->page_hdr->parent = INVALID_PAGE_ID;
        new_root->page_hdr->next_free_page_no = IX_NO_PAGE;
        new_root->insert_pair(0, old_node->get_key(0), (Rid){old_node->get_page_no(), -1});
        new_root->insert_pair(1, new_node->get_key(0), (Rid){new_node->get_page_no(), -1});
        maintain_child(new_root, 0);
        maintain_child(new_root, 1);
        update_root_page_no(new_root->get_page_no());
        buffer_pool_manager_->unpin_page(new_root->get_page_id(), true);
    } else {
        IxNodeHandle *parent_node = fetch_node(old_node->get_parent_page_no());
        int nums = parent_node->insert(key, (Rid){new_node->get_page_no(), 0});
        if(nums == parent_node->get_max_size()){
            auto new_parent_next = split(parent_node);
            auto new_key = new_parent_next->get_key(0);
            insert_into_parent(parent_node, new_key, new_parent_next, transaction);
            buffer_pool_manager_->unpin_page(new_parent_next->get_page_id(), true);
            buffer_pool_manager_->unpin_page(parent_node->get_page_id(), true);
        } else{
            buffer_pool_manager_->unpin_page(parent_node->get_page_id(), true);
        }
    }
}
```
产生bug的万恶之源之一。这个函数的目的是在分裂后，向上找到原节点的父亲节点
但这就会有一种特殊情况：如果原节点是根节点，他就没有父亲节点
首先考虑简单的非根节点情况，直接调用`fetch`获取原节点的父亲节点，并调用自身插回去。注意每处理完一个页面`unpin`一次
对于根节点情况，需要新建一个节点作为一个新根并初始化，之后在新根的0号位置插入旧节点的第一个键值对，在1号位置插入新节点的第一个键值对，之后调用`maintain_child`函数维护新根的孩子结点，最后更新整个文件根节点页面的编号，这里可以直接调用`update_root_page_no`

`insert_entry`函数：
```C++
page_id_t IxIndexHandle::insert_entry(const char *key, const Rid &value, Transaction *transaction) {
    // Todo:
    // 1. 查找key值应该插入到哪个叶子节点
    // 2. 在该叶子节点中插入键值对
    // 3. 如果结点已满，分裂结点，并把新结点的相关信息插入父节点
    // 提示：记得unpin page；若当前叶子节点是最右叶子节点，则需要更新file_hdr_.last_leaf；记得处理并发的上锁
    std::scoped_lock lock{root_latch_};
    auto leaf = find_leaf_page(key, Operation::INSERT, transaction);
    int nums = leaf.first->insert(key, value);
    int idx = leaf.first->lower_bound(key);
    if(!idx)
        maintain_parent(leaf.first);
    if(nums == leaf.first->get_max_size()){
        auto new_node = split(leaf.first);
        auto new_key = new_node->get_key(0);
        insert_into_parent(leaf.first, new_key, new_node, transaction);
        buffer_pool_manager_->unpin_page(new_node->get_page_id(), true);
        buffer_pool_manager_->unpin_page(leaf.first->get_page_id(), true);
    } else{
        buffer_pool_manager_->unpin_page(leaf.first->get_page_id(), true);
    }
    return leaf.first->get_page_no();
}
```
这个函数实际上是一个完整插入过程的封装：先查找叶子节点，然后插入，之后分裂并更新相关数据的完整过程
值得注意的是特殊情况的处理：对于`idx`等于0的情况。这个情况指的是要插入的键值对应该插入到叶子节点的起始位置，这会导致父亲节点应该插入的键值对发生变化，需要维护父节点，也就是调用`maintain_parent`函数。
对于叶子节点满了的情况，调用split函数分裂结点，并调用`insert_into_parent`函数插入父节点。注意这里的插入父节点的过程，会递归地向上维护父节点，直到父节点的键值对数不超过最大值。
unpin和latchct的处理，参照前文(累似了已经)
`delete_entry`函数：
```C++
bool IxIndexHandle::delete_entry(const char *key, Transaction *transaction) {
    // Todo:
    // 1. 获取该键值对所在的叶子结点
    // 2. 在该叶子结点中删除键值对
    // 3. 如果删除成功需要调用CoalesceOrRedistribute来进行合并或重分配操作，并根据函数返回结果判断是否有结点需要删除
    // 4. 如果需要并发，并且需要删除叶子结点，则需要在事务的delete_page_set中添加删除结点的对应页面；记得处理并发的上锁
    std::scoped_lock lock{root_latch_};
    auto leaf = find_leaf_page(key, Operation::DELETE, transaction);
    int nums_before_delete = leaf.first->page_hdr->num_key;
    int nums_after_delete = leaf.first->remove(key);
    int idx = leaf.first->lower_bound(key);
    if(nums_after_delete == nums_before_delete){
        return false;
    }
    if(!idx)
        maintain_parent(leaf.first);
    bool is_delete = coalesce_or_redistribute(leaf.first, transaction, &leaf.second);
    if(!is_delete)
        buffer_pool_manager_->unpin_page(leaf.first->get_page_id(), true);
    return true;
}
```
与`insert_entry`类似，一个完整的删除流程：先查找叶子节点，然后删除，之后合并/重分配并更新相关数据的完整过程。同样对于idx为0的情况，需要维护父节点

coalesce_or_redistribute函数：
```C++
bool IxIndexHandle::coalesce_or_redistribute(IxNodeHandle *node, Transaction *transaction, bool *root_is_latched) {
    // Todo:
    // 1. 判断node结点是否为根节点
    //    1.1 如果是根节点，需要调用AdjustRoot() 函数来进行处理，返回根节点是否需要被删除
    //    1.2 如果不是根节点，并且不需要执行合并或重分配操作，则直接返回false，否则执行2
    // 2. 获取node结点的父亲结点
    // 3. 寻找node结点的兄弟结点（优先选取前驱结点）
    // 4. 如果node结点和兄弟结点的键值对数量之和，能够支撑两个B+树结点（即node.size+neighbor.size >=
    // NodeMinSize*2)，则只需要重新分配键值对（调用Redistribute函数）
    // 5. 如果不满足上述条件，则需要合并两个结点，将右边的结点合并到左边的结点（调用Coalesce函数）
    if(node->is_root_page()){
        return adjust_root(node);
    } else if(node->page_hdr->num_key >= node->get_min_size()){
        return false;
    }
    auto parent = fetch_node(node->get_parent_page_no());
    IxNodeHandle *neighbor = nullptr;
    // char *key = node->get_key(0);
    int idx = parent->find_child(node);
    if(!idx){
        neighbor = fetch_node(parent->get_rid(idx+1)->page_no);
    } else{
        neighbor = fetch_node(parent->get_rid(idx-1)->page_no);

    }
    int total_num = node->page_hdr->num_key + neighbor->page_hdr->num_key;
    if(total_num >= node->get_min_size() * 2){
        redistribute(neighbor, node, parent, idx);
        buffer_pool_manager_->unpin_page(neighbor->get_page_id(), true);
        buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
        return false;
    } else{
        bool is_delete_parent = coalesce(&neighbor, &node, &parent, idx, transaction, root_is_latched);
        buffer_pool_manager_->unpin_page(neighbor->get_page_id(), true);
        if(!is_delete_parent)
            buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
        return true;
    }
}
```
第二个实验最容易出错的函数
这个函数实际上也是若干操作的组合，目的是在删除某个键值对后,B+树要重新调整节点时候使用。对于删除情况，有可能要合并，也有可能要重分配
合并情况：节点中键值对的个数小于节点最小键值对数量要求(一般是m/2)
重分配情况：
当一个节点的兄弟节点可以借出一个孩子或者键值对时，我们可以通过重分配操作来避免合并操作。
但是根节点和其他节点不一样，它是不受这个规则约束的，因此对于根节点我们要特殊处理
那么根据这个思路，首先写非根节点情况：根据这个节点的父亲节点找到它的兄弟节点，判定是否可以通过重分配操作来避免合并，如果可以，直接调用redistribute函数，并及时释放使用的节点的页面
如果不行的话，就需要调用coalesce来合并，并返回父亲节点是否需要删除
那么对于根节点情况，调用adjust_root函数
我的建议是，不要深究这个函数的流程，特定问题特定分析

adjust_root函数
```C++
bool IxIndexHandle::adjust_root(IxNodeHandle *old_root_node) {
    // Todo:
    // 1. 如果old_root_node是内部结点，并且大小为1，则直接把它的孩子更新成新的根结点
    // 2. 如果old_root_node是叶结点，且大小为0，则直接更新root page
    // 3. 除了上述两种情况，不需要进行操作
    if(old_root_node->is_leaf_page() == false){
        if(old_root_node->page_hdr->num_key == 1){
            auto new_root = fetch_node(old_root_node->get_rid(0)->page_no);
            update_root_page_no(new_root->get_page_no());
            new_root->page_hdr->parent = INVALID_PAGE_ID;
            buffer_pool_manager_->unpin_page(new_root->get_page_id(), true);
            return true;
        }
    }else {
        if(!old_root_node->page_hdr->num_key)
            return true;
    }
    return false;
}
```
这个是coalesce_or_redistribute函数中针对根节点的操作。值得注意的是，在B+树中，根节点既可以是内部节点（一般情况）也可以是叶节点（键值对数量非常少的情况，高度为1）
如果要调整的根节点是内部节点，大小为1，这意味着我们需要降低树的高度，直接将根节点的唯一孩子节点设为新的根节点。
如果是叶结点，那么这个节点就变成了一个空节点。在B+树中，我们通常不允许存在空的叶节点，因为这样的节点没有任何实际的用途，也不符合B+树的定义（即所有的叶子节点至少有一个键值对）。直接删掉(返回true)


```C++
void IxIndexHandle::redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index) {
    // Todo:
    // 1. 通过index判断neighbor_node是否为node的前驱结点
    // 2. 从neighbor_node中移动一个键值对到node结点中
    // 3. 更新父节点中的相关信息，并且修改移动键值对对应孩字结点的父结点信息（maintain_child函数）
    // 注意：neighbor_node的位置不同，需要移动的键值对不同，需要分类讨论
    if(index == 0){
        node->insert_pair(node->page_hdr->num_key, neighbor_node->get_key(0), *neighbor_node->get_rid(0));
        neighbor_node->erase_pair(0);
        parent->set_key(index+1, neighbor_node->get_key(0));
        maintain_child(node, node->get_size()-1);
    } else{
        int neighbor_position = neighbor_node->get_size() - 1;
        node->insert_pair(0, neighbor_node->get_key(neighbor_position), *neighbor_node->get_rid(neighbor_position));
        neighbor_node->erase_pair(neighbor_position);
        parent->set_key(index, neighbor_node->get_key(neighbor_position));
        maintain_child(node, 0);
    }
}
```
这个是coalesce_or_redistribute函数中的重分配操作。绝大多数情况下，都是寻找这个节点的前驱节点，然后重新分配键值对，但是对于这个节点已经是第一个节点（不存在前驱）的情况下，得找其后继节点（else情况）
无论是寻找前驱节点还是后继节点，原理都是互通的，就是把前一个节点的最后一个键值对插入到后一个节点的开始，同时要使用set_key,用这个键值对更新父亲节点的键值对,然后使用maintain_child更新孩子节点的父节点信息

coalesce函数：
```C++
bool IxIndexHandle::coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                             Transaction *transaction, bool *root_is_latched) {
    // Todo:
    // 1. 用index判断neighbor_node是否为node的前驱结点，若不是则交换两个结点，让neighbor_node作为左结点，node作为右结点
    // 2. 把node结点的键值对移动到neighbor_node中，并更新node结点孩子结点的父节点信息（调用maintain_child函数）
    // 3. 释放和删除node结点，并删除parent中node结点的信息，返回parent是否需要被删除
    // 提示：如果是叶子结点且为最右叶子结点，需要更新file_hdr_.last_leaf
    if(index == 0){
        std::swap(*node, *neighbor_node);
    }
    int num_before_coalesce = (*neighbor_node)->get_size();
    (*neighbor_node)->insert_pairs(num_before_coalesce, (*node)->get_key(0), (*node)->get_rid(0), (*node)->get_size());
    int num_after_coalesce = (*neighbor_node)->get_size();
    for(int i = num_before_coalesce; i < num_after_coalesce; i++){
        maintain_child(*neighbor_node, i);
    }
    if((*node)->is_leaf_page() && (*node)->get_page_no() == file_hdr_->last_leaf_){
        file_hdr_->last_leaf_ = (*neighbor_node)->page_hdr->prev_leaf;
    }
    release_node_handle(**node);
    if(index)
        (*parent)->erase_pair(index);
    else
        (*parent)->erase_pair(index+1);
    file_hdr_->num_pages_--;
    return coalesce_or_redistribute(*parent, transaction, root_is_latched);
}

```
这个函数为合并用函数，其实提示说的蛮清楚的，首先得用index判断neighbor_node是否为node的前驱结点，若不是则交换两个结点，让neighbor_node作为左结点，node作为右结点。之后把右节点键值对全部插入左节点，用maintain_child函数维护孩子节点的父节点信息。
然后就是特殊情况分析：
 如果node节点是叶子节点，并且是最右叶子节点：在这种情况下，我们需要更新文件头中的最后叶子节点信息。我们将文件头中的最后叶子节点设置为neighbor_node节点的前一个叶子节点。
之后要调用release_node_handle()来释放和删除node节点。
然后之后要擦除。如果neighbor_node是node的前驱节点（也就是说，neighbor_node在node的左边），那么我们需要删除parent节点的第index个键。否则（也就是说，neighbor_node在node的右边），我们需要删除parent节点的第index+1个键。

lower_bound函数：
```C++
Iid IxIndexHandle::lower_bound(const char *key) {

    auto leaf = find_leaf_page(key, Operation::FIND, nullptr);
    int idx = leaf.first->lower_bound(key);
    return {leaf.first->get_page_no(), idx};
}
```
我觉得主要得说清楚这个函数和以前的lower_bound函数的区别
IxIndexHandle::lower_bound(const char *key)函数是在整个B+树中查找给定键的下界，而IxNodeHandle::lower_bound(const char *target) const函数是在一个具体的节点中查找给定键的下界。

upper_bound函数：
```C++
Iid IxIndexHandle::upper_bound(const char *key) {
    auto leaf = find_leaf_page(key, Operation::FIND, nullptr);
    int idx = leaf.first->upper_bound(key);
    return {leaf.first->get_page_no(), idx};
}

```

