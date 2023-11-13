//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(PageGuardTest, SampleTest) {
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  guarded_page.Drop();

  EXPECT_EQ(0, page0->GetPinCount());

  {
    auto *page2 = bpm->NewPage(&page_id_temp);
    page2->RLatch();
    auto guard2 = ReadPageGuard(bpm.get(), page2);
  }

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, MyTest1) {
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;

  // New page / Unpin page
  {
    auto  *page0 = bpm->NewPage(&page_id_temp);
    auto  *page1 = bpm->FetchPage(page_id_temp);

    EXPECT_EQ(2, page0->GetPinCount());
    EXPECT_EQ(2, page1->GetPinCount());

    bpm->UnpinPage(page0->GetPageId(), false);
    EXPECT_EQ(1, page1->GetPinCount());

    bpm->UnpinPage(page1->GetPageId(), false);
    EXPECT_EQ(0, page0->GetPinCount());
  }

  // Basic page guard / auto Unpin page
  {
    BasicPageGuard basic_page_guard = bpm->NewPageGuarded(&page_id_temp);
  }

  auto *page0 = bpm->FetchPage(page_id_temp);
  EXPECT_EQ(1, page0->GetPinCount());

  {
    BasicPageGuard basic_page_guard = bpm->NewPageGuarded(&page_id_temp);
    auto *page1 = bpm->FetchPage(page_id_temp);
    EXPECT_EQ(2, page1->GetPinCount());
  }

  EXPECT_EQ(1, page0->GetPinCount());

  bpm->UnpinPage(page0->GetPageId(), false);
  EXPECT_EQ(0, page0->GetPinCount());

  // Read page guard
  {
    ReadPageGuard read_page_guard = bpm->FetchPageBasic(page0->GetPageId()).UpgradeRead();
    EXPECT_EQ(1, page0->GetPinCount());
  }
  EXPECT_EQ(0, page0->GetPinCount());

  {
    ReadPageGuard read_page_guard = bpm->FetchPageBasic(page0->GetPageId()).UpgradeRead();
    EXPECT_EQ(1, page0->GetPinCount());

    read_page_guard.Drop();
    EXPECT_EQ(0, page0->GetPinCount());
  }
  EXPECT_EQ(0, page0->GetPinCount());

  // Write page guard
  {
    WritePageGuard write_page_guard = bpm->FetchPageBasic(page0->GetPageId()).UpgradeWrite();
    EXPECT_EQ(1, page0->GetPinCount());
  }
  EXPECT_EQ(0, page0->GetPinCount());

  {
    WritePageGuard write_page_guard = bpm->FetchPageBasic(page0->GetPageId()).UpgradeWrite();
    EXPECT_EQ(1, page0->GetPinCount());

    write_page_guard.Drop();
    EXPECT_EQ(0, page0->GetPinCount());
  }
  EXPECT_EQ(0, page0->GetPinCount());

}


}  // namespace bustub
