#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

// why not
#include "parser/syntax_tree_printer.h"
extern "C" {
int yyparse(void);
//FILE *yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
// here
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  // todo:: use shared_ptr for schema
  if (ast->type_ == kNodeSelect)
    delete planner.plan_->OutputSchema();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(("./databases/" + db_name).c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  if (db_name == current_db_)
    current_db_ = "";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
    // get table name and columns information
    string table_name = ast->child_->val_;
    vector<Column *> columns;

    pSyntaxNode column_node = ast->child_->next_->child_;
    pSyntaxNode column_node_for_pri = ast->child_->next_->child_;
    int index = 0;
    bool isPrimaryKey = false;
    unordered_set<string> pri_key_set;
    vector<string> uniqueKeys;
    while (column_node_for_pri != nullptr) {
      if (column_node_for_pri->type_ != kNodeColumnList) {
        column_node_for_pri = column_node_for_pri->next_;
        continue;
      }
      for (auto get_pri = column_node_for_pri->child_; get_pri != nullptr; get_pri = get_pri->next_) {
        pri_key_set.insert(get_pri->val_);
      }
      break;
    }


    while (column_node != nullptr) {
      if (column_node->type_ == kNodeColumnDefinition) {
        Column *new_column;

        std::string columnName = column_node->child_->val_;
        std::string columnType = column_node->child_->next_->val_;
        bool isUnique = (column_node->val_ == "unique") | (pri_key_set.find(columnName) != pri_key_set.end());
        if (isUnique) {
          uniqueKeys.emplace_back(columnName);
        }
        TypeId type;
        if (columnType == "int") {
          type = TypeId::kTypeInt;
        } else if (columnType == "float") {
          type = TypeId::kTypeFloat;
        } else if (columnType == "char") {
          type = TypeId::kTypeChar;
        } else {
          type = TypeId::kTypeInvalid;
        }
        if (type == TypeId::kTypeInvalid) {
          return DB_FAILED;
        } else if (type == TypeId::kTypeChar) {
            std::string columnLength = column_node->child_->next_->child_->val_;
            if (columnLength.find('-') != string::npos || columnLength.find('.') != string::npos) {
              cout << "char length type wrong !" << endl;
              return DB_FAILED;
            }
            int length = std::stoi(columnLength);
            new_column = new Column(columnName, type, length, index, false, isUnique);
        } else {
          new_column = new Column(columnName, type, index, false, isUnique);
        }
        index++;
        columns.push_back(new_column);
        column_node = column_node->next_;
      } else if (column_node->type_ == kNodeColumnList) {
        isPrimaryKey = (column_node->val_ == "primary keys");
        break;
      }

    }

    // check if the table already exists
    if (current_db_.empty()) {
        cout << "No database selected" << endl;
        return DB_FAILED;
    }

    TableInfo* new_table = TableInfo::Create();
    Schema *schema = new Schema(columns, true); // what's is_manage
    dberr_t err = context->GetCatalog()->CreateTable(table_name, schema, context->GetTransaction(), new_table);
    if (err != DB_SUCCESS) {
        return err;
    }


    if (isPrimaryKey) {
        // create index for primaryKey
      IndexInfo *indexInfo;
      vector<string> pri_keys;

      for (auto pri_key = column_node->child_; pri_key != nullptr; pri_key = pri_key->next_) {
        pri_keys.emplace_back(pri_key->val_);
      }
      err = context->GetCatalog()->CreateIndex(table_name, table_name + "_pri_key_index", pri_keys, context->GetTransaction(), indexInfo, "bptree");
      if (err != DB_SUCCESS) {
        return err;
      }
      // create index for uniqueKey
      for (auto uniqueKey : uniqueKeys) {
        string index_name = table_name + "_unique_index_";
        index_name += uniqueKey;
        err = context->GetCatalog()->CreateIndex(table_name, index_name, uniqueKeys, context->GetTransaction(), indexInfo, "bptree");
        if (err != DB_SUCCESS) {
          return err;
        }
      }
    }
    cout << "Table created successfully" << endl;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif

    string table_name = ast->child_->val_;

    if (current_db_.empty()) {
        cout << "No database selected" << endl;
        return DB_FAILED;
    }

    dberr_t err = context->GetCatalog()->DropTable(table_name);
    if (err != DB_SUCCESS) {
        return err;
    }

    vector<IndexInfo *> index_infos;
    context->GetCatalog()->GetTableIndexes(table_name, index_infos);
    for (auto index_info : index_infos) {
        err = context->GetCatalog()->DropIndex(table_name, index_info->GetIndexName());
        if (err != DB_SUCCESS) {
        return err;
        }
    }

    cout << "Table "<< table_name << " dropped successfully" << endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif

    string table_name = ast->child_->val_;

    if (current_db_.empty()) {
        cout << "No database selected" << endl;
        return DB_FAILED;
    }

    TableInfo* new_table = TableInfo::Create();

    if (dbs_[current_db_]->catalog_mgr_->GetTable(table_name, new_table) != DB_SUCCESS) {
        cout << "Table does not exists" << endl;
        return DB_NOT_EXIST;
    }

    vector<IndexInfo*> indexes;
    if (dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, indexes) != DB_SUCCESS) {
        cout << "Failed to get indexes" << endl;
    }

    // Display the indexes
    if (indexes.empty()) {
        cout << "No indexes found for table" << table_name << endl;
    } else {
        cout << "Indexes for table " << table_name << ":" << endl;
        for (const auto& index : indexes) {
            // I don't know how to display it, leave it alone
            cout << "Index Name: " << index->GetIndexName() << endl;
        }
    }

    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif

    // extract, can be changed
    string index_name = ast->child_->val_;
    string table_name = ast->child_->next_->val_;
    string index_type = "bptree";
    vector<string> column_names;

    for (auto node = ast->child_->next_->next_->child_; node != nullptr; node = node->next_) {
        column_names.emplace_back(node->val_);
    }

    if (ast->child_->next_->next_->next_ != nullptr) {
        index_type = ast->child_->next_->next_->next_->val_;
    }

    TableInfo *tableInfo;
    dberr_t err = context->GetCatalog()->GetTable(table_name, tableInfo);
    if (err != DB_SUCCESS) {
        return err;
    }

    IndexInfo *indexInfo;
    err = context->GetCatalog()->CreateIndex(table_name, index_name, column_names, context->GetTransaction(), indexInfo, index_type);
    if (err != DB_SUCCESS) {
        return err;
    }

    // get original field
    auto row_begin = tableInfo->GetTableHeap()->Begin(context->GetTransaction());
    auto row_end = tableInfo->GetTableHeap()->End();
    for (auto row_iter = row_begin; row_iter != row_end; row_iter++) {
      auto rid = (*row_iter).GetRowId();
      vector<Field> fields;
      for (auto col : indexInfo->GetIndexKeySchema()->GetColumns()) {
            fields.push_back(*(*row_iter).GetField(col->GetTableInd()));
      }
      Row row_index(fields);
      err = indexInfo->GetIndex()->InsertEntry(row_index, rid, context->GetTransaction());
      if (err != DB_SUCCESS) {
            return err;
      }
    }
    cout << index_name << " created successfully !" << endl;
    return DB_SUCCESS;

}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
    if (current_db_.empty()) {
        cout << "No database selected" << endl;
        return DB_FAILED;
    }

    string index_name = ast->child_->val_;
    string table_name;
    vector<TableInfo *> tableInfos;
    dberr_t err = context->GetCatalog()->GetTables(tableInfos);
    if (err != DB_SUCCESS) {
        return err;
    }

    bool findIndex = false;
    for (auto tableInfo : tableInfos) {
        IndexInfo *indexInfo;
        err = context->GetCatalog()->GetIndex(tableInfo->GetTableName(), index_name, indexInfo);
        if (err == DB_SUCCESS) {
            findIndex = true;
            table_name = tableInfo->GetTableName();
            break;
        }
    }

    if (!findIndex) {
        cout << "Index " << index_name << " not exist !" << endl;
        return DB_INDEX_NOT_FOUND;
    }

    err = context->GetCatalog()->DropIndex(table_name, index_name);
    if (err != DB_SUCCESS) {
        return err;
    }
    cout << "Index "<< index_name << " dropped successfully" << endl;
    return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
    string filename = ast->child_->val_;

    ifstream infile(filename);
    if (!infile.is_open()) {
        cout << "Failed to open file: " << filename << endl;
        return DB_FAILED;
    }
    const int buf_size = 1024;
    char cmd[buf_size];
    memset(cmd, 0, buf_size);
    char ch;
    int cnt = 0;
    while (infile.get(ch)) {
        cmd[cnt++] = ch;
        if (ch == ';') {
            infile.get(ch);
            YY_BUFFER_STATE bp = yy_scan_string(cmd);
            if (bp == nullptr) {
              LOG(ERROR) << "Failed to create yy buffer state." << endl;
              return DB_FAILED;
            }
            yy_switch_to_buffer(bp);
            MinisqlParserInit();
            yyparse();
            if (MinisqlParserGetError()) {
              printf("%s\n", MinisqlParserGetErrorMessage());
              return DB_FAILED;
            }

            auto result = Execute(MinisqlGetParserRootNode());
            if (result == DB_FAILED) {
              return DB_FAILED;
            }
            MinisqlParserFinish();
            yy_delete_buffer(bp);
            yylex_destroy();

            ExecuteInformation(result);
            if (result == DB_QUIT) {
              break;
            }
            memset(cmd, 0, buf_size);
            cnt = 0;
        }
    }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
    for (auto& db : dbs_) {
        delete db.second;
    }
    dbs_.clear();
 return DB_QUIT;
}
