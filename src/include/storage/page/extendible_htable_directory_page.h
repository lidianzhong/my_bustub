//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.h
//
// Identification: src/include/storage/page/extendible_htable_directory_page.h
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * Directory page format:
 *  --------------------------------------------------------------------------------------
 * | MaxDepth (4) | GlobalDepth (4) | LocalDepths (512) | BucketPageIds(2048) | Free(1528)
 *  --------------------------------------------------------------------------------------
 */

#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "common/config.h"
#include "storage/index/generic_key.h"

namespace bustub {

static constexpr uint64_t HTABLE_DIRECTORY_PAGE_METADATA_SIZE = sizeof(uint32_t) * 2;

/**
 * 这段解释的就是 HTABLE_DIRECTORY_ARRAY_SIZE 表示 directory 页中容纳的 page_id 数量是512.
 * 否则如果是 1024，那么将没有 空间存放其它变量
 *
 * HTABLE_DIRECTORY_ARRAY_SIZE is the number of page_ids that can fit in the directory page of an extendible hash index.
 * This is 512 because the directory array must grow in powers of 2, and 1024 page_ids leaves zero room for
 * storage of the other member variables.
 */
static constexpr uint64_t HTABLE_DIRECTORY_MAX_DEPTH = 9;
static constexpr uint64_t HTABLE_DIRECTORY_ARRAY_SIZE = 1 << HTABLE_DIRECTORY_MAX_DEPTH;

/**
 * 目录页数据结构
 * Directory Page for extendible hash table.
 */
class ExtendibleHTableDirectoryPage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  ExtendibleHTableDirectoryPage() = delete;
  DISALLOW_COPY_AND_MOVE(ExtendibleHTableDirectoryPage);

  /**
   * 在从 buffer pool 中创建一个新 directory page 后，必须调用初始化方法设置默认值
   *
   * After creating a new directory page from buffer pool, must call initialize
   * method to set default values
   * @param max_depth Max depth in the directory page
   */
  void Init(uint32_t max_depth = HTABLE_DIRECTORY_MAX_DEPTH);

  /**
   * 将 hash 值转换为 bucket_page_ids[] 索引下标
   * Get the bucket index that the key is hashed to
   *
   * @param hash the hash of the key
   * @return bucket index current key is hashed to
   */
  auto HashToBucketIndex(uint32_t hash) const -> uint32_t;

  /**
   * 用 directory index 查找 bucket page
   * Lookup a bucket page using a directory index
   *
   * @param bucket_idx the index in the directory to lookup
   * @return bucket page_id corresponding to bucket_idx
   */
  auto GetBucketPageId(uint32_t bucket_idx) const -> page_id_t;

  /**
   * 用 bucket index 和 page_id 更新 directory index
   * // FIXME directory index ?
   * Updates the directory index using a bucket index and page_id
   *
   * @param bucket_idx directory index at which to insert page_id
   * @param bucket_page_id page_id to insert
   */
  void SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id);

  /**
   * 获取 split image 的索引
   * 传入 directory index 索引，返回 split image 的 directory index。
   *
   * Gets the split image of an index
   *
   * @param bucket_idx the directory index for which to find the split image
   * @return the directory index of the split image
   **/
  auto GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t;

  /**
   * 这个函数的作用是 返回一个掩码，这个掩码从低到高由 global_depth 个1和 剩下全是0组成
   *
   * 如何将 key 映射到目录索引？
   * 目录索引 = Hash(key) & 掩码
   *
   * GetGlobalDepthMask - returns a mask of global_depth 1's and the rest 0's.
   *
   * In Extendible Hashing we map a key to a directory index
   * using the following hash + mask function.
   *
   * DirectoryIndex = Hash(key) & GLOBAL_DEPTH_MASK
   *
   * where GLOBAL_DEPTH_MASK is a mask with exactly GLOBAL_DEPTH 1's from LSB
   * upwards.  For example, global depth 3 corresponds to 0x00000007 in a 32-bit
   * representation.
   *
   * @return mask of global_depth 1's and the rest 0's (with 1's from LSB upwards)
   */
  auto GetGlobalDepthMask() const -> uint32_t;

  /**
   * 得到 bucket index 的局部深度掩码
   *
   * GetLocalDepthMask - same as global depth mask, except it
   * uses the local depth of the bucket located at bucket_idx
   *
   * @param bucket_idx the index to use for looking up local depth
   * @return mask of local 1's and the rest 0's (with 1's from LSB upwards)
   */
  auto GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t;

  /**
   * 获取 directory 的全局深度
   * Get the global depth of the hash table directory
   *
   * @return the global depth of the directory
   */
  auto GetGlobalDepth() const -> uint32_t;

  auto GetMaxDepth() const -> uint32_t;

  /**
   * 增加 directory 的 global depth
   *
   * Increment the global depth of the directory
   */
  void IncrGlobalDepth();

  /**
   * 减少 directory 的 global depth
   *
   * Decrement the global depth of the directory
   */
  void DecrGlobalDepth();

  /**
   * 如果 directory 能被收缩，返回 true
   *
   * @return true if the directory can be shrunk
   */
  auto CanShrink() -> bool;

  /**
   * 返回当前 directory 的大小
   *
   * @return the current directory size
   */
  auto Size() const -> uint32_t;

  /**
   * 返回 directory 的最大大小
   *
   * @return the max directory size
   */
  auto MaxSize() const -> uint32_t;

  /**
   *
   * 获取 bucket_idx 对应的 bucket 的 local depth
   *
   * Gets the local depth of the bucket at bucket_idx
   *
   * @param bucket_idx the bucket index to lookup
   * @return the local depth of the bucket at bucket_idx
   */
  auto GetLocalDepth(uint32_t bucket_idx) const -> uint32_t;

  /**
   *
   *
   * Set the local depth of the bucket at bucket_idx to local_depth
   *
   * @param bucket_idx bucket index to update
   * @param local_depth new local depth
   */
  void SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth);

  /**
   * Increment the local depth of the bucket at bucket_idx
   * @param bucket_idx bucket index to increment
   */
  void IncrLocalDepth(uint32_t bucket_idx);

  /**
   * Decrement the local depth of the bucket at bucket_idx
   * @param bucket_idx bucket index to decrement
   */
  void DecrLocalDepth(uint32_t bucket_idx);

  /**
   * VerifyIntegrity
   *
   * Verify the following invariants:
   * (1) All LD <= GD.
   * (2) Each bucket has precisely 2^(GD - LD) pointers pointing to it.
   * (3) The LD is the same at each index with the same bucket_page_id
   */
  void VerifyIntegrity() const;

  /**
   * Prints the current directory
   */
  void PrintDirectory() const;

 private:
  uint32_t max_depth_;
  uint32_t global_depth_{0};
  uint8_t local_depths_[HTABLE_DIRECTORY_ARRAY_SIZE];
  page_id_t bucket_page_ids_[HTABLE_DIRECTORY_ARRAY_SIZE];
};

static_assert(sizeof(page_id_t) == 4);

static_assert(sizeof(ExtendibleHTableDirectoryPage) == HTABLE_DIRECTORY_PAGE_METADATA_SIZE +
                                                           HTABLE_DIRECTORY_ARRAY_SIZE +
                                                           sizeof(page_id_t) * HTABLE_DIRECTORY_ARRAY_SIZE);

static_assert(sizeof(ExtendibleHTableDirectoryPage) <= BUSTUB_PAGE_SIZE);

}  // namespace bustub
