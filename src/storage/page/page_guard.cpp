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
  if (this->page_ == nullptr || this->page_->GetPinCount() <= 0) {
    return;
  }

 // pin_count --
  this->bpm_->UnpinPage(this->PageId(), this->is_dirty_);

  // 使当前的 BasicPageGuard 失效
  this->bpm_ = nullptr;
  this->page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // 如果 this 对象已经被 Unpinned 掉了，那么 this 对之前的 page 直接释放保护
  // 如果 this 对象还没被 Unpinned 掉，那么就先 Unpinned 掉之前 page
  if (this->bpm_ != nullptr && this->page_ != nullptr) {
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
