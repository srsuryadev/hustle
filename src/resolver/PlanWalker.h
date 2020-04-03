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
  static std::shared_ptr<Table> walk(const std::shared_ptr<Plan>& plan);

  static std::shared_ptr<Table> walkQuery(
      const std::shared_ptr<QueryOperator>& query_operator);

  static std::shared_ptr<hustle::operators::SelectOperator> walkSelect(
      const std::shared_ptr<Expr>& expr);

  static std::shared_ptr<hustle::operators::Join> walkJoin(
      const std::shared_ptr<Comparative>& comparative);

  static std::shared_ptr<hustle::operators::Aggregate> walkAggregate(
      const std::shared_ptr<AggFunc>& aggregate_func,
      const std::vector<std::shared_ptr<ColumnReference>>& groupby_cols);

  static std::shared_ptr<hustle::operators::OrderBy> walkOrderBy(
      const std::vector<std::shared_ptr<ColumnReference>>& orderby_cols,
      const std::vector<OrderByDirection>& orders);

  static std::string walkColumnReference(
      const std::shared_ptr<ColumnReference>& col_ref) {
    return col_ref->column_name;
  }

  static arrow::compute::Datum walkLiteral(const std::shared_ptr<Expr>& expr);

  static arrow::compute::CompareOperator walkCompareOperator(
      ComparativeType comparative_type);

  static hustle::operators::AggregateKernels walkAggFuncType(
      AggFuncType agg_func_type);


};

}
}

#endif //HUSTLE_SRC_RESOLVER_PLANWALKER_H_
