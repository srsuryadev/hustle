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
        return walkQuery(curr->query_operator);
      }
      default: {
        std::cerr << "Walker except Query not implemented yet." << std::endl;
        return nullptr;
      }
    }
  }

  std::shared_ptr<Table> walkQuery(
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
        auto table = walkQuery(curr->input);

        std::shared_ptr<hustle::operators::SelectOperator> select = nullptr;
        for (auto &filter : curr->filter) {
          if (select == nullptr) {
            select = walkSelectFilter(filter);
          } else {
            auto curr_select = walkSelectFilter(filter);
            select = std::make_shared<hustle::operators::SelectComposite>(
                std::move(select),
                std::move(curr_select),
                hustle::operators::FilterOperator::AND);
          }
        }
        return select->runOperator({table});
      }
      case types::QueryOperatorType::Project: {
        auto curr = std::dynamic_pointer_cast<Project>(query_operator);
        auto table = walkQuery(curr->input);

        // TODO: Implement hustle::operators::Project
        // auto project = std::make_shared<hustle::operators::Project>();
        return project->runOperator(table);
      }
      case types::QueryOperatorType::Join: {
        auto curr = std::dynamic_pointer_cast<Join>(query_operator);
        auto left = walkQuery(curr->left_input);
        auto right = walkQuery(curr->right_input);

        auto join = walkJoin(curr->join_pred[0]); // assume only one join pred
        return join->hash_join(left, right);
      }
      case types::QueryOperatorType::GroupBy: {
        auto curr = std::dynamic_pointer_cast<GroupBy>(query_operator);
        auto table = walkQuery(curr->input);

        // TODO: Implement hustle::operators::GroupBy
        // auto groupby = std::make_shared<hustle::operators::GroupBy>();
        // return groupby->runOperator(table);
        return nullptr;
      }
      case types::QueryOperatorType::OrderBy: {
        auto curr = std::dynamic_pointer_cast<OrderBy>(query_operator);
        auto table = walkQuery(curr->input);

        // TODO: Implement hustle::operators::OrderBy
        // auto orderby = std::make_shared<hustle::operators::OrderBy>();
        // return orderby->runOperator(table);
        return nullptr;
      }
    }
  }

  static std::shared_ptr<hustle::operators::SelectOperator> walkSelectFilter(
      const std::shared_ptr<Expr>& expr) {
    switch (expr->type) {
      case ExprType::Disjunctive: {
        auto curr = std::dynamic_pointer_cast<Disjunctive>(expr);
        return std::make_shared<hustle::operators::SelectComposite>(
            walkSelectFilter(curr->exprs[0]),
            walkSelectFilter(curr->exprs[1]),
            hustle::operators::FilterOperator::OR);
      }
      case ExprType::Comparative: {
        auto curr = std::dynamic_pointer_cast<Comparative>(expr);
        return std::make_shared<hustle::operators::Select>(
            walkCompareOperator(curr->op),
            walkColumnReference(curr->left),
            walkLiteral(curr->right));
      }
      default: {
        std::cerr << "ExprType::" << expr->type._to_string()
                  << " not supported in walkSelectFilter" << std::endl;
        exit(-1);
      }
    }
  }

  static std::shared_ptr<hustle::operators::Join> walkJoin(
      std::shared_ptr<Comparative> comparative) {
    assert(comparative->right->type == +ExprType::ColumnReference);
    return std::make_shared<hustle::operators::Join>(
        walkColumnReference(comparative->left),
        walkColumnReference(
            std::dynamic_pointer_cast<ColumnReference>(comparative->right)));
  }

  static std::string walkColumnReference(
      const std::shared_ptr<ColumnReference>& col_ref) {
    return col_ref->column_name;
  }

  static arrow::compute::Datum walkLiteral(const std::shared_ptr<Expr>& expr) {
    switch (expr->type) {
      case ExprType::IntLiteral: {
        auto curr = std::dynamic_pointer_cast<IntLiteral>(expr);
        return arrow::compute::Datum((int64_t) curr->value);
      }
      case ExprType::StrLiteral: {
        auto curr = std::dynamic_pointer_cast<StrLiteral>(expr);
        return arrow::compute::Datum((char*) curr->value.c_str());
      }
      default: {
        std::cerr << "ExprType::" << expr->type._to_string()
                  << " not supported in walkLiteral" << std::endl;
        exit(-1);
      }
    }
  }

  static arrow::compute::CompareOperator walkCompareOperator(
      ComparativeType comparative_type) {
    switch (comparative_type) {
      case ComparativeType::NE:
        return arrow::compute::CompareOperator::NOT_EQUAL;
      case ComparativeType::EQ:
        return arrow::compute::CompareOperator::EQUAL;
      case ComparativeType::GT:
        return arrow::compute::CompareOperator::GREATER;
      case ComparativeType::LE:
        return arrow::compute::CompareOperator::LESS_EQUAL;
      case ComparativeType::LT:
        return arrow::compute::CompareOperator::LESS;
      case ComparativeType::GE:
        return arrow::compute::CompareOperator::GREATER_EQUAL;
    }
  }



};

}
}

#endif //HUSTLE_SRC_RESOLVER_PLANWALKER_H_
