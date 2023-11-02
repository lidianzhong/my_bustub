//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
//  throw NotImplementedException(
//      "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the "
//      "throw exception line in `disk_scheduler.cpp`.");

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  request_queue_.Put(std::move(r));
}

void DiskScheduler::StartWorkerThread() {
    while (true) {
      std::optional<DiskRequest> disk_request = request_queue_.Get();
      if (!disk_request.has_value()) {
        return ;
      }
      if (disk_request->is_write_) {
        disk_manager_->WritePage(disk_request->page_id_, disk_request->data_);
      } else {
        disk_manager_->ReadPage(disk_request->page_id_, disk_request->data_);
      }

      disk_request->callback_.set_value(true);
    }
}

}  // namespace bustub
