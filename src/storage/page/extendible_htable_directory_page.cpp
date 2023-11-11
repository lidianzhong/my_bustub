//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  BUSTUB_ASSERT(max_depth > 0 && max_depth <= HTABLE_DIRECTORY_MAX_DEPTH, "out of range");
  max_depth_ = max_depth;
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  uint32_t global_depth_mask = GetGlobalDepthMask();
  return hash & global_depth_mask;
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  // 获取 bucket index 在 local_depth_ 下的 分裂图下标
  uint32_t local_depth = local_depths_[bucket_idx];
  return bucket_idx ^ (1 << local_depth);  // FIXME local_depth or -1 ?
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  // 开始启用更高的一位
  global_depth_ ++;

  // 填充分裂图中数据
  uint32_t origin_index = 0;
  uint32_t new_index = 1 << (global_depth_ - 1);
  while (new_index < 1 << global_depth_) {
    bucket_page_ids_[new_index] = bucket_page_ids_[origin_index];
    local_depths_[new_index] = local_depths_[origin_index];
    new_index ++;
    origin_index ++;
  }

}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  // 减少一位比特位
  global_depth_ --;
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  // 收缩的条件: 桶中的所有 local_depth_ 严格小于 global_depth_ 时
  for (uint32_t depth: local_depths_) {
    if (depth >= global_depth_) {
      return false;
    }
  }

  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {
  // 返回当前 directory page 的大小
  return 1 << global_depth_;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  local_depths_[bucket_idx] ++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  local_depths_[bucket_idx] --;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return (1 << global_depth_) - 1; }

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  // 获取 local_depth 的掩码
  uint32_t local_depth_mask = bucket_page_ids_[bucket_idx];
  return (1 << local_depth_mask) - 1;
}
auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }
auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t {
  return max_depth_;
}

}  // namespace bustub
