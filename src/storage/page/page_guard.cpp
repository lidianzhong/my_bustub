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
  // 设置 page 为可逐出的
  if (this->page_->GetPinCount() != 0) {
    this->bpm_->UnpinPage(this->PageId(), this->is_dirty_);
  }

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
  return {this->bpm_, this->page_};
}
auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  this->page_->WLatch();
  return {this->bpm_, this->page_};
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { this->guard_ = std::move(that.guard_); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  this->guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  // 设置 page 为可逐出的
  if (this->guard_.page_->GetPinCount() != 0) {
    this->guard_.bpm_->UnpinPage(this->PageId(), this->guard_.is_dirty_);
  }

  this->guard_.page_->RUnlatch();

  // 使当前对象失效
  this->guard_.page_ = nullptr;
  this->guard_.bpm_ = nullptr;
}

ReadPageGuard::~ReadPageGuard() {
  if (this->guard_.page_ != nullptr && this->guard_.bpm_ != nullptr) {
    this->Drop();
  }
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { this->guard_ = std::move(that.guard_); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  this->guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  // 设置 page 为可逐出的
  if (this->guard_.page_->GetPinCount() != 0) {
    this->guard_.bpm_->UnpinPage(this->PageId(), this->guard_.is_dirty_);
  }

  this->guard_.page_->WUnlatch();

  // 使当前对象失效
  this->guard_.page_ = nullptr;
  this->guard_.bpm_ = nullptr;
}

WritePageGuard::~WritePageGuard() {
  if (this->guard_.page_ != nullptr && this->guard_.bpm_ != nullptr) {
    this->Drop();
  }
}  // NOLINT

}  // namespace bustub
