#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages):cache(num_pages, v_list.end()){
  num = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(v_list.empty()){
    return false;
  }
  (*frame_id) = v_list.back();
  cache[(*frame_id)] = v_list.end();
  v_list.pop_back();
  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  auto it = cache[frame_id];
  if(it != v_list.end()){
    v_list.erase(it);
    cache[frame_id] = v_list.end();
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if(v_list.size() >= num || cache[frame_id] != v_list.end()){
    return;
  }
  v_list.push_front(frame_id);
  cache[frame_id] = v_list.begin();
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return v_list.size();
}