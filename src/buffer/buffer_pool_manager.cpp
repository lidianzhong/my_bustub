
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // 初始时，所有 page 都在 free list 中
  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::unique_lock<std::mutex> lock(latch_);

  page_id_t new_page_id;

  // 获取 frame_id
  frame_id_t frame_id;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();

  } else if (replacer_->Evict(&frame_id)) {
    // 对已经逐出的 page 进行处理
    Page *old_page = &pages_[frame_id];

    if (old_page->IsDirty()) {
      FlushPage(old_page->GetPageId());
    }

    page_table_.erase(old_page->GetPageId());

  } else {
    return nullptr;
  }

  // 准备新 page_id
  new_page_id = this->AllocatePage();

  // 加入 replacer_ 计算
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  // 添加映射关系
  page_table_[new_page_id] = frame_id;

  // 在内存创建实体结构
  Page *page = &pages_[frame_id];
  page->ResetMemory();
  page->pin_count_ = 1;
  page->page_id_ = new_page_id;
  page->is_dirty_ = false;

  *page_id = new_page_id;

  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::unique_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;

  // 尝试从内存中获取数据
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;
    pages_[frame_id].is_dirty_ = true;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }

  // 从磁盘中读取数据
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();

  } else if (replacer_->Evict(&frame_id)) {
    Page *old_page = &pages_[frame_id];

    if (old_page->IsDirty()) {
      FlushPage(old_page->GetPageId());
    }

    page_table_.erase(old_page->GetPageId());

  } else {
    return nullptr;
  }

  // 加入 replacer_ 处理
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  // 添加映射关系
  page_table_[page_id] = frame_id;

  // 读取磁盘内容
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({false, pages_[frame_id].data_, page_id, std::move(promise)});
  future.get();

  // 更新内存状态
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;

  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end() || pages_[page_table_[page_id]].GetPinCount() == 0) {
    return false;
  }

  Page *page = &pages_[page_table_[page_id]];

  page->pin_count_--;

  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(page_table_[page_id], true);
  }

  page->is_dirty_ = is_dirty;

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "Flush Page is not allow INVALID_PAGE_ID page");

  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  // 获取 frame_id
  frame_id_t frame_id = page_table_[page_id];

  // 获取 page
  Page *page = &pages_[frame_id];

  // 更新内存状态
  page->is_dirty_ = false;

  // 写入磁盘中
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, page->data_, page_id, std::move(promise)});
  future.get();

  return true;
}

void BufferPoolManager::FlushAllPages() {}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }

  Page *page = &pages_[page_table_[page_id]];
  if (page->GetPinCount() > 0) {
    return false;
  }

  if (page->is_dirty_) {
    FlushPage(page_id);
  }

  frame_id_t frame_id = page_table_[page_id];

  page->ResetMemory();
  page_table_.erase(page->GetPageId());
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
