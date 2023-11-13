#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  // 复制状态
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;

  // 使之前的 BasicPageGuard 失效
  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
  BUSTUB_ASSERT(this->page_ != nullptr, "try to drop an already dropped page guard");
  BUSTUB_ASSERT(this->page_->GetPinCount() > 0, "try to drop the page whose pin count is already 0");

 // pin_count --
  this->bpm_->UnpinPage(this->PageId(), this->is_dirty_);

  // 使当前的 BasicPageGuard 失效
  this->bpm_ = nullptr;
  this->page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // 如果当前对象还守护着页面，需要将当前页面保护关掉
  if (this->page_->GetPinCount() != 0) {
    this->bpm_->UnpinPage(this->PageId(), this->is_dirty_);
  }

  // 转移资源
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;

  that.page_ = nullptr;
  that.bpm_ = nullptr;

  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  if (this->bpm_ != nullptr && this->page_ != nullptr) {
    this->Drop();
  }
}  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  this->page_->RLatch();

  ReadPageGuard read_page_guard = {this->bpm_, this->page_};

  this->bpm_ = nullptr;
  this->page_ = nullptr;

  return read_page_guard;
}
auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  this->page_->WLatch();

  WritePageGuard write_page_guard = {this->bpm_, this->page_};

  this->bpm_ = nullptr;
  this->page_ = nullptr;

  return write_page_guard;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { this->guard_ = std::move(that.guard_); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  this->guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {

  this->guard_.page_->RUnlatch();

  this->guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  if (this->guard_.page_ != nullptr && this->guard_.bpm_ != nullptr) {
    this->guard_.page_->RUnlatch();
  }
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { this->guard_ = std::move(that.guard_); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  this->guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {

  this->guard_.page_->WUnlatch();

  this->guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  if (this->guard_.page_ != nullptr && this->guard_.bpm_ != nullptr) {
    this->guard_.page_->WUnlatch();
  }
}  // NOLINT

}  // namespace bustub
