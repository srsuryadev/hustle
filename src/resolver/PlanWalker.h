#ifndef HUSTLE_SRC_RESOLVER_PLANWALKER_H_
#define HUSTLE_SRC_RESOLVER_PLANWALKER_H_

#include <table/table.h>
#include <table/util.h>
#include <operators/Select.h>
#include <operators/Join.h>
#include <operators/Aggregate.h>
#include "Resolver.h"

namespace hustle {
namespace resolver {

class PlanWalker {
 public:
  std::shared_ptr<Table> walk(const std::shared_ptr<Plan>& plan) {
    switch (plan->type) {
      case types::PlanType::Query: {
        auto curr = std::dynamic_pointer_cast<Query>(plan);
        return walk_Query(curr->query_operator);
      }
      default: {
        std::cerr << "Walker except Query not implemented yet." << std::endl;
        return nullptr;
      }
    }
  }

  std::shared_ptr<Table> walk_Query(
      const std::shared_ptr<QueryOperator>& query_operator) {
    switch (query_operator->type) {
      case types::QueryOperatorType::TableReference: {
        auto curr = std::dynamic_pointer_cast<TableReference>(query_operator);

        // TODO: Link to catalog
        std::string filename = std::to_string(curr->i_table) + curr->table_name;
        return read_from_file(filename.c_str());
      }
      case types::QueryOperatorType::Select: {
        auto curr = std::dynamic_pointer_cast<Select>(query_operator);
        std::shared_ptr<Table> table = walk_Query(curr->input);

        // TODO(Lichengxi): Change to take in an Expr?
        for (auto& filter : curr->filter) {
          auto select = std::make_shared<hustle::operators::Select>(filter);
          table = select->runOperator({table});
        }
        return table;
      }
      case types::QueryOperatorType::Project: {
        auto curr = std::dynamic_pointer_cast<Project>(query_operator);
        std::shared_ptr<Table> table = walk_Query(curr->input);

        // TODO: Implement hustle::operators::Project
        auto project = std::make_shared<hustle::operators::Project>();
        return project->runOperator(table);
      }
      case types::QueryOperatorType::Join: {
        auto curr = std::dynamic_pointer_cast<Join>(query_operator);
        std::shared_ptr<Table> left = walk_Query(curr->left_input);
        std::shared_ptr<Table> right = walk_Query(curr->right_input);

        // TODO(Lichengxi): Change to take in a vector of Exprs?
        auto join = std::make_shared<hustle::operators::Join>(curr->join_pred);
        return join->hash_join(left, right);
      }
      case types::QueryOperatorType::GroupBy: {
        auto curr = std::dynamic_pointer_cast<GroupBy>(query_operator);
        std::shared_ptr<Table> table = walk_Query(curr->input);

        // TODO: Implement hustle::operators::GroupBy
        auto groupby = std::make_shared<hustle::operators::GroupBy>();
        return groupby->runOperator(table);
      }
      case types::QueryOperatorType::OrderBy: {
        auto curr = std::dynamic_pointer_cast<OrderBy>(query_operator);
        std::shared_ptr<Table> table = walk_Query(curr->input);

        // TODO: Implement hustle::operators::OrderBy
        auto orderby = std::make_shared<hustle::operators::OrderBy>();
        return orderby->runOperator(table);

      }
    }
  }


};

}
}

#endif //HUSTLE_SRC_RESOLVER_PLANWALKER_H_
