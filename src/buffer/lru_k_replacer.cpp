//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::unique_lock<std::mutex> lock(this->latch_);
  size_t mx_k_distance =0;
  size_t early_timestamp;
  frame_id_t evict_id = -1;

  for (const auto &i : node_store_) {
    auto node = i.second;
    if (!node->GetEvictable()) {
      continue;
    }

    size_t now_k_distance = node->GetKDistance(this->current_timestamp_);
    if (now_k_distance > mx_k_distance) {
      mx_k_distance = now_k_distance;
      early_timestamp = node->GetEarlyTimestamp();
      evict_id = i.first;
    } else if (now_k_distance == mx_k_distance) {
      size_t now_early_timestamp = node->GetEarlyTimestamp();
      if (early_timestamp > now_early_timestamp) {
        early_timestamp = now_early_timestamp;
        evict_id = i.first;
      }
    }
  }

  if (evict_id == -1) {
    return false;
  }

  *frame_id = evict_id;
  node_store_.erase(evict_id);
  this->evictable_size_--;
  return true;

}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::unique_lock<std::mutex> lock(this->latch_);

  this->current_timestamp_++;
  BUSTUB_ASSERT(frame_id >= 0 && (size_t)frame_id < replacer_size_, "frame_id is out of range");

  if (node_store_.find(frame_id) == node_store_.end()) {
    node_store_[frame_id] = std::make_shared<LRUKNode>(frame_id, this->current_timestamp_, this->k_);
  } else {
    node_store_[frame_id]->RecordAccess(this->current_timestamp_);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock<std::mutex> lock(this->latch_);

  if (node_store_.find(frame_id) == node_store_.end()) {
    BUSTUB_ASSERT(false, "frame_id not found");
  }

  auto now_frame = node_store_[frame_id];
  if (now_frame->GetEvictable() != set_evictable) {
    if (set_evictable) {
      this->evictable_size_++;
    } else {
      this->evictable_size_--;
    }
    now_frame->SetEvictable(set_evictable);
  }

}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::mutex> lock(this->latch_);

  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }

  if (node_store_[frame_id]->GetEvictable()) {
    this->evictable_size_--;
    node_store_.erase(frame_id);
  } else {
    BUSTUB_ASSERT(false, "frame id is not evictable");
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::unique_lock<std::mutex> lock(this->latch_);
  return evictable_size_;
}


LRUKNode::LRUKNode(frame_id_t fid, size_t current_timestamp_, size_t k) : fid_(fid), k_(k) {
  this->history_.push_front(current_timestamp_);
}
auto LRUKNode::GetEvictable() -> bool { return this->is_evictable_; }
void LRUKNode::SetEvictable(bool set_evictable) { this->is_evictable_ = set_evictable; }
auto LRUKNode::GetKDistance(size_t current_timestamp_) -> size_t {
  if (this->history_.size() < k_) {
    return INF;
  }

  size_t k_tmp = 1;
  auto it = this->history_.begin();
  while (k_tmp < this->k_) {
    k_tmp++;
    it++;
  }
  return current_timestamp_ - *it;
}
auto LRUKNode::GetEarlyTimestamp() -> size_t { return this->history_.back(); }
void LRUKNode::RecordAccess(size_t current_timestamp_) { this->history_.push_front(current_timestamp_); }

}  // namespace bustub
