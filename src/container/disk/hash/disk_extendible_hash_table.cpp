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
  // TODO name parameter is not implemented.

  // 在创建哈希表时，就将唯一一个 header page 创建好
  BasicPageGuard header_guard = bpm_->NewPageGuarded(&header_page_id_);

  // 创建好后，就要调用初始化方法
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth_);

  // 在创建哈希表时，就将创建一个 directory page
  page_id_t directory_page_id = INVALID_PAGE_ID;
  BasicPageGuard directory_guard = bpm_->NewPageGuarded(&directory_page_id);

  // 创建好后，就要调用初始化方法
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory_page->Init(directory_max_depth_);

  // 在创建哈希表时，就将创建一个 bucket page
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  BasicPageGuard bucket_guard = bpm_->NewPageGuarded(&bucket_page_id);

  // 创建好后，就要调用初始化方法
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(bucket_max_size_);

  // header page 中放置 directory page
  header_page->SetDirectoryPageId(0, directory_page_id);

  // directory page 中放置 bucket page
  directory_page->SetBucketPageId(0, bucket_page_id);
  directory_page->SetLocalDepth(0, 0);

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
  BasicPageGuard header_page_guard = bpm_->FetchPageBasic(GetHeaderPageId());
  auto header_page = header_page_guard.AsMut<ExtendibleHTableHeaderPage>();
  uint32_t directory_index = header_page->HashToDirectoryIndex(hash_key);
  auto directory_page_id = static_cast<page_id_t>(header_page->GetDirectoryPageId(directory_index));

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

  // 先将值哈希
  uint32_t hash_key = Hash(key);

  // 找到应该插入的 bucket page
  // 1. 获取到 directory page id
  BasicPageGuard header_page_guard = bpm_->FetchPageBasic(GetHeaderPageId());
  auto header_page = header_page_guard.AsMut<ExtendibleHTableHeaderPage>();
  uint32_t directory_index = header_page->HashToDirectoryIndex(hash_key);
  auto directory_page_id = static_cast<page_id_t>(header_page->GetDirectoryPageId(directory_index));

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

    // 先获取个新的 bucket page, 拿到 bucket page id
    page_id_t split_image_page_id = INVALID_PAGE_ID;
    WritePageGuard split_image_guard = bpm_->NewPageGuarded(&split_image_page_id).UpgradeWrite();
    auto split_image_page = split_image_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    split_image_page->Init(bucket_max_size_);

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
      // (3) 将 split image index 指向新 bucket
      uint32_t split_image_index = directory_page->GetSplitImageIndex(bucket_index);

      directory_page->SetBucketPageId(split_image_index, split_image_page_id);
    }
    // 1. (2) 如果 global depth < local depth
    else if (directory_page->GetGlobalDepth() < directory_page->GetLocalDepth(bucket_index)) {
      // Local depth increase && Bucket Split
      // (1) local depth 增加 1
      directory_page->IncrLocalDepth(bucket_index);

      // (2) 将 split image index 指向新 bucket
      uint32_t split_image_index = directory_page->GetSplitImageIndex(bucket_index);

      directory_page->SetBucketPageId(split_image_index, split_image_page_id);
    }

    // 无论是哪种情况，只要 bucket 是满的，就需要重新散列
    uint32_t bucket_size = bucket_page->Size();
    for (uint32_t i = 0; i < bucket_size; ++i) {
      const K i_key = bucket_page->KeyAt(i);
      if (bucket_index == directory_page->HashToBucketIndex(Hash(i_key))) {
        continue;
      }

      // 重新计算的索引值 和 当前的 bucket_index 不一样，需要移动
      // 1. 获取之前的值
      const V i_value = bucket_page->ValueAt(i);
      // 1. 把之前的 bucket 中的值删了
      bucket_page->Remove(i_key, cmp_);
      // 2. 添加到新的 split image bucket 中
      split_image_page->Insert(i_key, i_value, cmp_);
    }

    // 将新的值插入 bucket 中
    if (bucket_index == directory_page->HashToBucketIndex(hash_key)) {
      bucket_page->Insert(key, value, cmp_);
    } else {
      split_image_page->Insert(key, value, cmp_);
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
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
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

  // 2. 从 buffer pool manager 中拿到 directory page
  WritePageGuard directory_page_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();

  // 3. 从 directory page 中拿到 bucket page
  uint32_t bucket_index = directory_page->HashToBucketIndex(hash_key);
  WritePageGuard bucket_page_guard = bpm_->FetchPageWrite(directory_page->GetBucketPageId(bucket_index));
  auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  // 移除 bucket page 中的元素

  bool status = bucket_page->Remove(key, cmp_);

  return status;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
