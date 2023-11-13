//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // TODO(user) name parameter is not implemented.

  // 从 buffer pool 中获取一个 page 当作 header page
  WritePageGuard header_page_guard = bpm_->NewPageGuarded(&header_page_id_).UpgradeWrite();
  auto *header_page = header_page_guard.AsMut<ExtendibleHTableHeaderPage>();

  // 调用初始化方法
  header_page->Init(header_max_depth_);

}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {

  // 先将值哈希
  uint32_t hash_key = Hash(key);

  // 找到应该插入的 bucket page
  // 1. 获取到 directory page id
  ReadPageGuard header_page_guard = bpm_->FetchPageRead(GetHeaderPageId());
  auto header_page = header_page_guard.As<ExtendibleHTableHeaderPage>();
  uint32_t directory_index = header_page->HashToDirectoryIndex(hash_key);
  auto directory_page_id = static_cast<page_id_t>(header_page->GetDirectoryPageId(directory_index));

  // 如果 key 哈希到一个空的 directory_page_id, 返回 false
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }

  // 2. 从 buffer pool manager 中拿到 directory page
  WritePageGuard directory_page_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();

  // 3. 从 directory page 中拿到 bucket page
  uint32_t bucket_index = directory_page->HashToBucketIndex(hash_key);
  WritePageGuard bucket_page_guard = bpm_->FetchPageWrite(directory_page->GetBucketPageId(bucket_index));
  auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  V value;
  if (bucket_page->Lookup(key, value, cmp_)) {
    result->push_back(value);
    return true;
  }

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  // 往 hash table 中插入 key-value

  // 先通过 Hash(key) 获取 directory_page_ids[] 下标对应 page_id
  // 根据 page_id 是否创建来执行 创建 directory page 或者 插入现有 directory page 中

  uint32_t hash_key = Hash(key);
  WritePageGuard header_page_guard = bpm_->FetchPageWrite(GetHeaderPageId());
  auto *header_page = header_page_guard.AsMut<ExtendibleHTableHeaderPage>();
  uint32_t directory_index = header_page->HashToDirectoryIndex(hash_key);
  auto directory_page_id = static_cast<page_id_t>(header_page->GetDirectoryPageId(directory_index));

  if (directory_page_id == INVALID_PAGE_ID) {
    return InsertToNewDirectory(header_page, directory_index, hash_key, key, value);
  }

  // 2. 从 buffer pool manager 中拿到 directory page
  WritePageGuard directory_page_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();

  // 3. 从 directory page 中拿到 bucket page
  uint32_t bucket_index = directory_page->HashToBucketIndex(hash_key);
  WritePageGuard bucket_page_guard = bpm_->FetchPageWrite(directory_page->GetBucketPageId(bucket_index));
  auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  // 查看 bucket page 是否已经满了
  if (bucket_page->IsFull()) {
    // bucket 满了，应该做两件事: 1. 处理溢出 2. 元素重新散列

    // 从 buffer pool 中获取 page 作为 new bucket page
    page_id_t new_bucket_page_id = INVALID_PAGE_ID;
    WritePageGuard new_bucket_page_guard = bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite();
    auto *new_bucket_page = new_bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket_page->Init(bucket_max_size_);
    uint32_t new_bucket_index = directory_page->GetSplitImageIndex(bucket_index);
    uint32_t new_local_depth_mask = directory_page->GetLocalDepthMask(bucket_index);

    // 1. 处理溢出
    // 1. (1) 如果 global depth == local depth
    if (directory_page->GetGlobalDepth() == directory_page->GetLocalDepth(bucket_index)) {

      // 走到这说明要扩张 directory ，在扩张前要判断一下有没有超过 directory max_depth_
      if (directory_page->Size() == directory_page->MaxSize()) {
        return false;
      }

      // Local depth increase && Directory Expansion && Bucket Split
      // (1) local depth 需要增加 1, 且在 IncrGlobalDepth() 之前
      directory_page->IncrLocalDepth(bucket_index);
      // (2) 扩展 directory 并且 复制指针
      directory_page->IncrGlobalDepth();
      // (3) 获取新 page 拿到 bucket_page_id && 初始化 bucket page

      // (4) 更新 directory
      uint32_t new_local_depth = directory_page->GetLocalDepth(bucket_index);
      this->UpdateDirectoryMapping(directory_page, new_bucket_index, new_bucket_page_id, new_local_depth, new_local_depth_mask);

    }
    // 1. (2) 如果 global depth < local depth
    else if (directory_page->GetGlobalDepth() < directory_page->GetLocalDepth(bucket_index)) {
      // Local depth increase && Bucket Split
      // (1) local depth 增加 1
      directory_page->IncrLocalDepth(bucket_index);

      // (2) 获取新 page 拿到 bucket_page_id && 初始化 bucket page

      // (3) 更新 directory
      uint32_t new_local_depth = directory_page->GetLocalDepth(bucket_index);
      UpdateDirectoryMapping(directory_page, new_bucket_index, new_bucket_page_id, new_local_depth, new_local_depth_mask);

    }

    // 重新散列
    MigrateEntries(bucket_page, new_bucket_page, new_bucket_index, new_local_depth_mask);

    // 将新的值插入 bucket 中
    if (new_bucket_index != (Hash(key) & new_local_depth_mask)) {
      bucket_page->Insert(key, value, cmp_);
    } else {
      new_bucket_page->Insert(key, value, cmp_);
    }

  } else {
    // bucket page 没有满，直接插入即可
    bucket_page->Insert(key, value, cmp_);
  }

  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  // 获取新 page 拿到 directory_page_id && 初始化 directory page && 尝试创建 new bucket, 并插入数据
  // 成功 更新 directory_page_ids[] && 返回 true
  // 失败 返回 false


 // 从 buffer pool 中获取 page 作为 new directory page
  page_id_t new_directory_page_id = INVALID_PAGE_ID;
  WritePageGuard new_directory_page_guard = bpm_->NewPageGuarded(&new_directory_page_id).UpgradeWrite();
  auto* new_directory_page = new_directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();

  // 调用初始化方法
  new_directory_page->Init(directory_max_depth_);

  // 尝试插入 new bucket

  if (InsertToNewBucket(new_directory_page, 0, key, value)) {
    // 更新 directory_page_ids[]
    header->SetDirectoryPageId(directory_idx, new_directory_page_id);
    return true;
  }

  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  // 获取新 page 拿到 bucket_page_id && 初始化 bucket page && 尝试插入数据
  // 成功 更新 bucket_page_ids[] && 更新 local_depth_[]
  // 失败 返回 false

  // 从 buffer pool 中获取 page 作为 new bucket page
  page_id_t new_bucket_page_id = INVALID_PAGE_ID;
  WritePageGuard new_bucket_page_guard = bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite();
  auto *new_bucket_page = new_bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  // 初始化 bucket page
  new_bucket_page->Init(bucket_max_size_);

  // 尝试插入数据
  if (new_bucket_page->Insert(key, value, cmp_)) {
    // 更新 bucket_page_ids[]
    directory->SetBucketPageId(bucket_idx, new_bucket_page_id);
    // 更新 local_depth_[]
    directory->SetLocalDepth(bucket_idx, 0);
    return true;
  }

  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
  directory->SetLocalDepth(new_bucket_idx, new_local_depth);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                    ExtendibleHTableBucketPage<K, V, KC> *new_bucket, uint32_t new_bucket_idx,
                    uint32_t local_depth_mask) {

  // 遍历旧桶元素，重新对元素进行散列
  uint32_t tmp_size = old_bucket->Size();
  for (uint32_t i = 0; i < tmp_size; i++) {
    const K i_key = old_bucket->KeyAt(i);
    if (new_bucket_idx != (Hash(i_key) & local_depth_mask)) {
      continue;
    }

    // 需要移动到 new bucket
    // 1. 获取 old bucket 中的值
    const V i_value = old_bucket->ValueAt(i);
    // 2. 删掉 old bucket 中的值
    old_bucket->Remove(i_key, cmp_);
    // 3. 添加到 new bucket 中
    new_bucket->Insert(i_key, i_value, cmp_);

    // 由于删除元素, tmp_size 的值需要减一
    tmp_size --;

  }

}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {

  // 先将值哈希
  uint32_t hash_key = Hash(key);

  // 找到应该插入的 bucket page
  // 1. 获取到 directory page id
  BasicPageGuard header_page_guard = bpm_->FetchPageBasic(GetHeaderPageId());
  auto header_page = header_page_guard.AsMut<ExtendibleHTableHeaderPage>();
  uint32_t directory_index = header_page->HashToDirectoryIndex(hash_key);
  auto directory_page_id = static_cast<page_id_t>(header_page->GetDirectoryPageId(directory_index));

  // 如果 key 哈希到一个空的 directory_page_id, 返回 false
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }

  // 2. 从 buffer pool manager 中拿到 directory page
  WritePageGuard directory_page_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();

  // 3. 从 directory page 中拿到 bucket page
  uint32_t bucket_index = directory_page->HashToBucketIndex(hash_key);
  WritePageGuard bucket_page_guard = bpm_->FetchPageWrite(directory_page->GetBucketPageId(bucket_index));
  auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  // 移除 bucket page 中的元素

  return bucket_page->Remove(key, cmp_);

}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
