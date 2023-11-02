//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

/**
 * BufferPoolManager 从它的内部 buffer pool 中读取 disk page。
 * BufferPoolManager reads disk pages to and from its internal buffer pool.
 */
class BufferPoolManager {
 public:
  /**
   * 创建一个新的 BufferPoolManager
   * 传入参数分别是 buffer pool 的大小，disk manager，LRU-K 算法中需要的常数 k
   * TODO: log_manager
   *
   * @brief Creates a new BufferPoolManager.
   * @param pool_size the size of the buffer pool
   * @param disk_manager the disk manager
   * @param replacer_k the LookBack constant k for the LRU-K replacer
   * @param log_manager the log manager (for testing only: nullptr = disable logging). Please ignore this for P1.
   */
  BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k = LRUK_REPLACER_K,
                    LogManager *log_manager = nullptr);

  /**
   * 销毁 BufferPoolManager
   * @brief Destroy an existing BufferPoolManager.
   */
  ~BufferPoolManager();

  /** 返回 buffer pool 的 size（也就是 frames 的数量） */
  /** @brief Return the size (number of frames) of the buffer pool. */
  auto GetPoolSize() -> size_t { return pool_size_; }

  /** 返回 buffer pool 所有 pages 的指针（pages_ 是一个指向 Page 类型的指针） */
  /** @brief Return the pointer to all the pages in the buffer pool. */
  auto GetPages() -> Page * { return pages_; }

  /**
   * TODO(P1): Add implementation
   *
   * 在  buffer pool 中创建一个新 page。
   * 除了所有的 frame 都在使用且不可逐出（也就是 page 是 pinned 状态）的时候将 page_id 设置为 nullptr，
   * 其余状态赋予 page_id 一个新的 page 的 id
   *
   * 你应该要么从 free list 中，要不从 replacer 中选择替换 frame（当 free list 没有可以用的了再从 replacer 中拿）。
   * 然后调用 AllocatePage() 方法获取一个新的 page_id。如果替换的 frame 是一个脏页，你应该先写回磁盘。你还需要重置新 page 的内存和数据
   * 记住通过调用 replacer.SetEvictable(frame_id, false) 来 Pinned frame，使得 buffer pool manager 在它 unpin 前不会逐出它
   *
   * 记得在 replacer 中记录 frame 访问历史，从而让 lru-k 工作。
   *
   * @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
   * are currently in use and not evictable (in another word, pinned).
   *
   * You should pick the replacement frame from either the free list or the replacer (always find from the free list
   * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
   * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
   *
   * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
   * so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
   * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
   *
   * @param[out] page_id id of created page
   * @return nullptr if no new pages could be created, otherwise pointer to new page
   */
  auto NewPage(page_id_t *page_id) -> Page *;

  /**
   * TODO(P2): Add implementation
   *
   * NewPage 的 Page Guard 包装器
   *
   * 功能应该与 NewPage 相同，只不过您返回的是 BasicPageGuard 结构，而不是返回指向页面的指针。
   *
   * @brief PageGuard wrapper for NewPage
   *
   * Functionality should be the same as NewPage, except that
   * instead of returning a pointer to a page, you return a
   * BasicPageGuard structure.
   *
   * @param[out] page_id, the id of the new page
   * @return BasicPageGuard holding a new page
   */
  auto NewPageGuarded(page_id_t *page_id) -> BasicPageGuard;

  /**
   * TODO(P1): Add implementation
   *
   * 从 buffer pool 中获取请求的 page。如果需要从磁盘中获取 page_id 但是所有的 frames 当前正在使用并且不可逐出时（也就是 pinned），返回 nullptr
   *
   * 首先从 buffer pool 中查找 page_id。
   * 如果没有找到，那就找一个替换的 frame，这个 frame 要不从 free list 里获取，要不通过 replacer 获取。
   * 然后通过使用 disk_scheduler_->Schedule() 调度读取 DiskRequest 从磁盘读取页面，并且替代 frame 中的旧 page。
   * 和 NewPage() 函数一样，如果这个 old page 是脏页，则需要将其写回磁盘并更新新页面的元数据
   *
   * 请记住禁用逐出并记录框架的访问历史记录，就像 NewPage() 一样。
   *
   *
   *
   * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
   * but all frames are currently in use and not evictable (in another word, pinned).
   *
   * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
   * the replacer (always find from the free list first), read the page from disk by scheduling a read DiskRequest with
   * disk_scheduler_->Schedule(), and replace the old page in the frame. Similar to NewPage(), if the old page is dirty,
   * you need to write it back to disk and update the metadata of the new page
   *
   * In addition, remember to disable eviction and record the access history of the frame like you did for NewPage().
   *
   * @param page_id id of page to be fetched
   * @param access_type type of access to the page, only needed for leaderboard tests.
   * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
   */
  auto FetchPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> Page *;

  /**
   * TODO(P2): Add implementation
   *
   * @brief PageGuard wrappers for FetchPage
   *
   * Functionality should be the same as FetchPage, except
   * that, depending on the function called, a guard is returned.
   * If FetchPageRead or FetchPageWrite is called, it is expected that
   * the returned page already has a read or write latch held, respectively.
   *
   * @param page_id, the id of the page to fetch
   * @return PageGuard holding the fetched page
   */
  auto FetchPageBasic(page_id_t page_id) -> BasicPageGuard;
  auto FetchPageRead(page_id_t page_id) -> ReadPageGuard;
  auto FetchPageWrite(page_id_t page_id) -> WritePageGuard;

  /**
   * TODO(P1): Add implementation
   *
   * 取消 pin buffer pool 中的目标 page。如果 page_id 不在 buffer pool 中或者这个 page 的 pin count 已经为 0，返回 false
   *
   * 减少这个 page 的 pin count 数。如果 pin count 的数量到了 0，frame 通过 replacer 设置为 evictable
   *
   * 另外，在页面上设置 dirty flag 以指示该页面是否被修改。
   *
   * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
   * 0, return false.
   *
   * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
   * Also, set the dirty flag on the page to indicate if the page was modified.
   *
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise
   * @param access_type type of access to the page, only needed for leaderboard tests.
   * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
   */
  auto UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type = AccessType::Unknown) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * 将 page 刷新到 disk，无论这个 page 是不是 dirty page。在刷新后取消 dirty page 的标志。
   *
   * 传入的 page 是要求不能为 INVALID_PAGE_ID
   *
   * 如果在页表中找不到该页，则为 false，否则为 true
   *
   * @brief Flush the target page to disk.
   *
   * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
   * Unset the dirty flag of the page after flushing.
   *
   * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
   * @return false if the page could not be found in the page table, true otherwise
   */
  auto FlushPage(page_id_t page_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * 将 buffer pool 中所有的 pages 刷新到磁盘
   *
   * @brief Flush all the pages in the buffer pool to disk.
   */
  void FlushAllPages();

  /**
   * TODO(P1): Add implementation
   *
   * 删除 buffer pool 中的 page。如果 page_id 不在 buffer pool，什么都不用做，返回 true 即可。
   * 如果 page 是 pinned 状态，不能被删除，立即返回 false。
   *
   * 在从页表中删除 page 后，停止在 replacer 中追踪这个 frame，并且将这个 frame 添加到 free list 中。
   * 同样，重置页面的内存和元数据。最后，你应该调用  DeallocatePage() 来模拟释放磁盘上的 page。
   *
   * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
   * page is pinned and cannot be deleted, return false immediately.
   *
   * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
   * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
   * imitate freeing the page on the disk.
   *
   * @param page_id id of page to be deleted
   * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
   */
  auto DeletePage(page_id_t page_id) -> bool;

 private:
  /** Number of pages in the buffer pool. */
  const size_t pool_size_;
  /** The next page id to be allocated  */
  std::atomic<page_id_t> next_page_id_ = 0;

  /** Array of buffer pool pages. */
  Page *pages_;
  /** Pointer to the disk sheduler. */
  std::unique_ptr<DiskScheduler> disk_scheduler_ __attribute__((__unused__));
  /** Pointer to the log manager. Please ignore this for P1. */
  LogManager *log_manager_ __attribute__((__unused__));
  /** Page table for keeping track of buffer pool pages. */
  std::unordered_map<page_id_t, frame_id_t> page_table_;
  /** Replacer to find unpinned pages for replacement. */
  std::unique_ptr<LRUKReplacer> replacer_;
  /** List of free frames that don't have any pages on them. */
  std::list<frame_id_t> free_list_;
  /** This latch protects shared data structures. We recommend updating this comment to describe what it protects. */
  std::mutex latch_;

  /**
   * @brief Allocate a page on disk. Caller should acquire the latch before calling this function.
   * @return the id of the allocated page
   */
  auto AllocatePage() -> page_id_t;

  /**
   * @brief Deallocate a page on disk. Caller should acquire the latch before calling this function.
   * @param page_id id of the page to deallocate
   */
  void DeallocatePage(__attribute__((unused)) page_id_t page_id) {
    // This is a no-nop right now without a more complex data structure to track deallocated pages
  }

  // TODO(student): You may add additional private members and helper functions
};
}  // namespace bustub
