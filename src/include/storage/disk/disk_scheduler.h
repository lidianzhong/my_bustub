//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.h
//
// Identification: src/include/storage/disk/disk_scheduler.h
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <future>  // NOLINT
#include <optional>
#include <thread>  // NOLINT

#include "common/channel.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

/**
 * DiskManager 需要执行的读取和写入的单个请求
 * @brief Represents a Write or Read request for the DiskManager to execute.
 */
struct DiskRequest {
  /** 标记这个请求是读操作还是写操作 */
  /** Flag indicating whether the request is a write or a read. */
  bool is_write_;

  /**
   *  指向 page 内存开始的指针
   *  Pointer to the start of the memory location where a page is either:
   *   1. being read into from disk (on a read).
   *   2. being written out to disk (on a write).
   */
  char *data_;

  /** 操作的 page id */
  /** ID of the page being read from / written to disk. */
  page_id_t page_id_;

  /** 当请求完成时，向请求发出者发送请求已完成的信号 */
  /** Callback used to signal to the request issuer when the request has been completed. */
  std::promise<bool> callback_;
};

/**
 * DiskScheduler 负责对磁盘的读和写操作进行调度
 *
 * 将需要被调度的 DiskRequest 当作参数传入 Schedule() 函数。
 * 调度器维护一个后台工作线程，通过调用 disk manager 来处理调度请求。
 * 后台工作线程在构造函数中创建，在析构函数销毁线程。
 *
 * @brief The DiskScheduler schedules disk read and write operations.
 *
 * A request is scheduled by calling DiskScheduler::Schedule() with an appropriate DiskRequest object. The scheduler
 * maintains a background worker thread that processes the scheduled requests using the disk manager. The background
 * thread is created in the DiskScheduler constructor and joined in its destructor.
 */
class DiskScheduler {
 public:
  explicit DiskScheduler(DiskManager *disk_manager);
  ~DiskScheduler();

  /**
   * TODO(P1): Add implementation
   * 从 DiskManager 中调度一个请求来执行，传入需要被调度的 request
   * @brief Schedules a request for the DiskManager to execute.
   *
   * @param r The request to be scheduled.
   */
  void Schedule(DiskRequest r);

  /**
   * TODO(P1): Add implementation
   *
   * 后台工作线程以处理调度请求
   *
   * 后台工作线程需要处理请求。从 DiskScheduler 开始到销毁。
   *
   * @brief Background worker thread function that processes scheduled requests.
   *
   * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
   * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
   */
  void StartWorkerThread();

  using DiskSchedulerPromise = std::promise<bool>;

  /**
   * 创建一个 Promise
   *
   * @brief Create a Promise object. If you want to implement your own version of promise, you can change this function
   * so that our test cases can use your promise implementation.
   *
   * @return std::promise<bool>
   */
  auto CreatePromise() -> DiskSchedulerPromise { return {}; };

 private:
  /** 指向的 disk manager */
  /** Pointer to the disk manager. */
  DiskManager *disk_manager_ __attribute__((__unused__));

  /** 共享队列，用来调度和处理请求。
   * 当 DiskScheduler 的析构函数被调用，将 `std::nullopt` 放入队列以发出停止执行的信号 */
  /** A shared queue to concurrently schedule and process requests. When the DiskScheduler's destructor is called,
   * `std::nullopt` is put into the queue to signal to the background thread to stop execution. */
  Channel<std::optional<DiskRequest>> request_queue_;
  /** 后台工作线程，向 disk manager 发送 scheduled 请求 */
  /** The background thread responsible for issuing scheduled requests to the disk manager. */
  std::optional<std::thread> background_thread_;
};
}  // namespace bustub
