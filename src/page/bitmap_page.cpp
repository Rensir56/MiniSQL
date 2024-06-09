#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
  //*分配一个空闲页，并通过page_offset返回所分配的空闲页位于该段中的下标（从0开始）；
  //*如果没有空闲页，返回false；
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
      // Iterate through the bytes to find the first unset bit (free page)
    for (uint32_t byte_index = 0; byte_index < MAX_CHARS; ++byte_index) {
        for (uint8_t bit_index = 0; bit_index < 8; ++bit_index) {
            if (IsPageFreeLow(byte_index, bit_index)) {
                // Set the bit indicating the page is now allocated
                bytes[byte_index] |= (1 << bit_index);
                page_offset = (byte_index * 8) + bit_index;
                ++page_allocated_;  // Increment the count of allocated pages
                return true;
            }
        }
    }
    // No free pages available
    return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;
  if (byte_index >= MAX_CHARS || bit_index >= 8 || IsPageFreeLow(byte_index, bit_index)) {
      // Out of bounds or page not allocated
      return false;
  }
  // Clear the bit indicating the page is now free
  bytes[byte_index] &= ~(1 << bit_index);
  --page_allocated_;  // Decrement the count of allocated pages
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
    if (byte_index > MAX_CHARS) {
        return false;  // Out of bounds
    }
    bool is_free = (((bytes[byte_index] >> bit_index) & 1) == 0 ? true : false);
    return is_free;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;