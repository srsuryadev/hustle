#ifndef HUSTLE_SRC_RESOLVER_PLANWALKER_H_
#define HUSTLE_SRC_RESOLVER_PLANWALKER_H_

#include <filesystem>

#include <table/table.h>
#include <table/util.h>
#include <operators/Select.h>
#include <operators/Join.h>
#include <operators/Aggregate.h>
#include <operators/OrderBy.h>
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

  static std::shared_ptr<Table> walkQuery(
      const std::shared_ptr<QueryOperator>& query_operator) {
    switch (query_operator->type) {
      case types::QueryOperatorType::TableReference: {
        auto curr = std::dynamic_pointer_cast<TableReference>(query_operator);

        // TODO: Link to catalog
        std::string filename = curr->table_name + ".hsl";
        // return read_from_file(filename.c_str());

        std::cout << "read_from_file " << filename << std::endl;
        return nullptr;
      }
      case types::QueryOperatorType::Select: {
        auto curr = std::dynamic_pointer_cast<Select>(query_operator);

        std::shared_ptr<hustle::operators::SelectOperator> select;
        for (auto &filter : curr->filter) {
          if (select == nullptr) {
            select = walkSelect(filter);
          } else {
            auto curr_select = walkSelect(filter);
            select = std::make_shared<hustle::operators::SelectComposite>(
                std::move(select),
                std::move(curr_select),
                hustle::operators::FilterOperator::AND);
          }
        }

        auto table = walkQuery(curr->input);
        // return select->run_operator({table});
        std::cout << "select->run_operator" << std::endl;
        return nullptr;
      }
      case types::QueryOperatorType::Project: {
        auto curr = std::dynamic_pointer_cast<Project>(query_operator);
        auto table = walkQuery(curr->input);

        // TODO: Implement hustle::operators::Project
        // auto project = std::make_shared<hustle::operators::Project>();
        // return project->run_operator(table);
        return nullptr;
      }
      case types::QueryOperatorType::Join: {
        auto curr = std::dynamic_pointer_cast<Join>(query_operator);
        auto left = walkQuery(curr->left_input);
        auto right = walkQuery(curr->right_input);

        auto join = walkJoin(curr->join_pred[0]); // assume only one join pred
        // return join->hash_join(left, arrow::compute::Datum(), right, arrow::compute::Datum());
        std::cout << "join->hash_join" << std::endl;
        return nullptr;
      }
      case types::QueryOperatorType::Aggregate: {
        auto curr = std::dynamic_pointer_cast<Aggregate>(query_operator);
        auto table = walkQuery(curr->input);
        auto aggregate = walkAggregate(curr->aggregate_func, curr->groupby_cols);
        // return aggregate->run_operator({table});
        std::cout << "aggregate->run_operator" << std::endl;
        return nullptr;
      }
      case types::QueryOperatorType::OrderBy: {
        auto curr = std::dynamic_pointer_cast<OrderBy>(query_operator);
        auto table = walkQuery(curr->input);

        // TODO: Implement hustle::operators::OrderBy
        auto orderby = walkOrderBy(curr->orderby_cols, curr->orders);
        // return orderby->run_operator(table);
        std::cout << "orderby->run_operator" << std::endl;
        return nullptr;
      }
    }
  }

  static std::shared_ptr<hustle::operators::SelectOperator> walkSelect(
      const std::shared_ptr<Expr>& expr) {
    switch (expr->type) {
      case ExprType::Disjunctive: {
        auto curr = std::dynamic_pointer_cast<Disjunctive>(expr);
        return std::make_shared<hustle::operators::SelectComposite>(
            walkSelect(curr->exprs[0]),
            walkSelect(curr->exprs[1]),
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
                  << " not supported in walkSelect" << std::endl;
        exit(-1);
      }
    }
  }

  static std::shared_ptr<hustle::operators::Join> walkJoin(
      const std::shared_ptr<Comparative>& comparative) {
    assert(comparative->right->type == +ExprType::ColumnReference);
    return std::make_shared<hustle::operators::Join>(
        walkColumnReference(comparative->left),
        walkColumnReference(
            std::dynamic_pointer_cast<ColumnReference>(comparative->right)));
  }

  static std::shared_ptr<hustle::operators::Aggregate> walkAggregate(
      const std::shared_ptr<AggFunc>& aggregate_func,
      const std::vector<std::shared_ptr<ColumnReference>>& groupby_cols) {

    std::vector<std::shared_ptr<arrow::Field>> aggregate_fields = {
        arrow::field(walkColumnReference(
            std::dynamic_pointer_cast<ColumnReference>(aggregate_func->expr)),
                     arrow::utf8())};

    std::vector<std::shared_ptr<arrow::Field>> group_by_fields = {};
    std::vector<std::shared_ptr<arrow::Field>> order_by_fields = {};
    return std::make_shared<hustle::operators::Aggregate>(
        walkAggFuncType(aggregate_func->func),
        aggregate_fields,
        group_by_fields,
        order_by_fields);
  }

  static std::shared_ptr<hustle::operators::OrderBy> walkOrderBy(
      const std::vector<std::shared_ptr<ColumnReference>>& orderby_cols,
      const std::vector<OrderByDirection>& orders) {

    // std::shared_ptr<arrow::Field> order_by_field =
    // return std::make_shared<hustle::operators::OrderBy>();
    return nullptr;

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

  static hustle::operators::AggregateKernels walkAggFuncType(
      AggFuncType agg_func_type) {
    switch (agg_func_type) {
      case AggFuncType::COUNT: return hustle::operators::AggregateKernels::COUNT;
      case AggFuncType::AVG: return hustle::operators::AggregateKernels::MEAN;
      case AggFuncType::SUM: return hustle::operators::AggregateKernels::SUM;
    }
  }



};

}
}

#endif //HUSTLE_SRC_RESOLVER_PLANWALKER_H_
