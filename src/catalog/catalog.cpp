#include "catalog/catalog.h"
#include "glog/logging.h"
void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  uint32_t serialize_size = 0;
  serialize_size += sizeof(uint32_t) * 3;
  serialize_size += table_meta_pages_.size() * (sizeof(table_id_t) + sizeof(page_id_t));
  serialize_size += index_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t));
  return serialize_size;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
//    ASSERT(false, "Not Implemented yet");
  if(init){
    catalog_meta_ = CatalogMeta::NewInstance();
  } else {
    Page *page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(page->GetData());
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
    for(auto iter:catalog_meta_->table_meta_pages_){
      LoadTable(iter.first,iter.second);
    }
    for(auto iter:catalog_meta_->index_meta_pages_){
      LoadIndex(iter.first,iter.second);
    }
    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();
  }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  if (table_names_.find(table_name) != table_names_.end()) {
    return DB_TABLE_ALREADY_EXIST;
  }
  page_id_t meta_data_page_id;
  //创建新表堆
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_);
  //创建新页面
  auto page = buffer_pool_manager_->NewPage(meta_data_page_id);
  page_id_t root_page_id = table_heap->GetFirstPageId();
  //创建新表元数据
  TableMetadata *table_metadata = TableMetadata::Create(next_table_id_, table_name, root_page_id, schema);
  table_metadata->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(meta_data_page_id, true);
  //更新目录元信息
  catalog_meta_->table_meta_pages_.emplace(next_table_id_, meta_data_page_id);
  table_names_.emplace(table_name,next_table_id_);
  //初始化表信息
  table_info = TableInfo::Create();
  table_info->Init(table_metadata, table_heap);
  tables_.emplace(next_table_id_, table_info);
  next_table_id_++;
  //更新目录元数据页面
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  auto table = table_names_.find(table_name);
  if(table == table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  table_info = tables_.find(table->second)->second;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  for(auto iter:tables_){
    tables.push_back(iter.second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  // ASSERT(false, "Not Implemented yet");
  if(table_names_.find(table_name) == table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  if (index_names_[table_name].find(index_name) != index_names_[table_name].end()) {
    return DB_INDEX_ALREADY_EXIST;
  }
  //获取表的ID和列信息
  table_id_t table_id = table_names_.find(table_name)->second;
  auto col = tables_[table_id]->GetSchema()->GetColumns();
  std::vector<uint32_t> key_map;
  int a;
  for(auto iter1:index_keys){
    a = 0;
    for (auto iter2 : col) {
      if(iter1 == iter2->GetName()){
        key_map.push_back(iter2->GetTableInd());
        a = 1;
        break;
      }
    }
    if(a == 0){
      return DB_COLUMN_NAME_NOT_EXIST;
    }
  }
  //创建新索引元数据页面
  page_id_t meta_data_page_id;
  auto index_page = buffer_pool_manager_->NewPage(meta_data_page_id);
  IndexMetadata *index_metadata = IndexMetadata::Create(next_index_id_, index_name, table_id, key_map);
  index_metadata->SerializeTo(index_page->GetData());
  buffer_pool_manager_->UnpinPage(meta_data_page_id, true);
  //初始化
  index_info = IndexInfo::Create();
  index_info->Init(index_metadata, tables_[table_id], buffer_pool_manager_);
  //更新目录元数据
  catalog_meta_->index_meta_pages_.emplace(next_index_id_, meta_data_page_id);
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  //更新索引映射名称
  if(index_names_.find(table_name) == index_names_.end()){
    std::unordered_map<std::string,index_id_t> new_index_names;
    new_index_names.emplace(index_name, next_index_id_);
    index_names_.emplace(table_name, new_index_names);
  }else{
    index_names_.find(table_name)->second.emplace(index_name, next_index_id_);
  }
  //更新索引映射ID
  indexes_.emplace(next_index_id_, index_info);
  next_index_id_++;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  //查找对应索引映射
  auto table = index_names_.find(table_name);
  if(table == index_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  auto index_f = table->second.find(index_name);
  if(index_f == table->second.end()){
    return DB_INDEX_NOT_FOUND;
  }
  //获取索引信息
  auto index = index_f->second;
  index_info = indexes_.find(index)->second;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  if(index_names_.find(table_name) == index_names_.end()){
    return DB_INDEX_NOT_FOUND;
  }
  //获取索引信息
  for(auto iter:index_names_.find(table_name)->second){
    indexes.push_back(indexes_.find(iter.second)->second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");
  if(table_names_.find(table_name) == table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  //删除相关索引
  if(index_names_.find(table_name) != index_names_.end()){
    std::unordered_map<std::string, index_id_t> index_map;
    index_map = index_names_[table_name];
    for (auto iter : index_map) {
      DropIndex(table_name, iter.first);
    }
  }
  //删除表的元数据信息
  table_id_t table_id = table_names_[table_name];
  table_names_.erase(table_name);
  tables_.erase(table_id);
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_[table_id]);
  catalog_meta_->table_meta_pages_.erase(table_id);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  if(index_names_.find(table_name) == index_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  auto table = index_names_[table_name];
  if(table.find(index_name) == table.end()){
    return DB_INDEX_NOT_FOUND;
  }
  //删除索引
  index_id_t index_id = table[index_name];
  index_names_[table_name].erase(index_name);
  indexes_.erase(index_id);
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_[index_id]);
  catalog_meta_->index_meta_pages_.erase(index_id);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  if(tables_.find(table_id)!=tables_.end()){
    return DB_FAILED;
  }
  //加载表格元数据
  auto page = buffer_pool_manager_->FetchPage(page_id);
  TableMetadata *table_metadata;
  TableMetadata::DeserializeFrom(page->GetData(), table_metadata);
  buffer_pool_manager_->UnpinPage(page_id, false);
  //创建表格实例
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_metadata->GetFirstPageId(), table_metadata->GetSchema(), log_manager_, lock_manager_);
  TableInfo *table_info = TableInfo::Create();
  table_info->Init(table_metadata, table_heap);
  //更新状态
  tables_.emplace(table_id, table_info);
  table_names_.emplace(table_metadata->GetTableName(), table_id);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  if(indexes_.find(index_id) != indexes_.end()){
    return DB_FAILED;
  }
  //加载索引元数据
  auto page = buffer_pool_manager_->FetchPage(page_id);
  IndexMetadata *index_metadata;
  IndexMetadata::DeserializeFrom(page->GetData(), index_metadata);
  buffer_pool_manager_->UnpinPage(page_id, false);
  //创建索引信息
  IndexInfo *index_info = IndexInfo::Create();
  index_info->Init(index_metadata, tables_[index_metadata->GetTableId()], buffer_pool_manager_);
  //更新索引名称映射
  std::string table = tables_.find(index_metadata->GetTableId())->second->GetTableName();
  if(index_names_.find(table) == index_names_.end()){
    std::unordered_map<std::string,index_id_t> new_index_names;
    new_index_names.emplace(index_metadata->GetIndexName(), index_id);
    index_names_.emplace(table, new_index_names);
}else{
  index_names_.find(table)->second.emplace(index_metadata->GetIndexName(), index_id);
}
//更新状态
indexes_.emplace(index_id, index_info);

return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  auto table = tables_.find(table_id); 
  if(table == tables_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  table_info = table->second;
  return DB_SUCCESS;
}