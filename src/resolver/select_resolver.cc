#include "resolver/select_resolver.h"

#include <memory>

namespace hustle {
namespace resolver {

using namespace hustle::operators;
using namespace hustle::catalog;

void SelectResolver::ResolveJoinPredExpr(Expr* pExpr) {
  if (pExpr == NULL) {
    return;
  }

  switch (pExpr->op) {
    case TK_STRING:
    case TK_INTEGER:
    case TK_COLUMN: {
      break;
    }
    case TK_EQ: {
      Expr* leftExpr = pExpr->pLeft;
      Expr* rightExpr = pExpr->pRight;
      ColumnReference lRef, rRef;

      if ((leftExpr != NULL && leftExpr->op == TK_COLUMN) &&
          (rightExpr != NULL && rightExpr->op == TK_COLUMN)) {
        lRef = {catalog_->getTable(leftExpr->iTable),
                leftExpr->y.pTab->aCol[leftExpr->iColumn].zName};
        rRef = {catalog_->getTable(rightExpr->iTable),
                rightExpr->y.pTab->aCol[rightExpr->iColumn].zName};
        JoinPredicate join_pred = {lRef, arrow::compute::EQUAL, rRef};
        join_predicates_.emplace_back(join_pred);
      }
      break;
    }
    default: {
      ResolveJoinPredExpr(pExpr->pLeft);
      ResolveJoinPredExpr(pExpr->pRight);
    }
  }
}

std::shared_ptr<PredicateTree> SelectResolver::ResolvePredExpr(Expr* pExpr) {
  if (pExpr == NULL) {
    return nullptr;
  }
  // std::cout << "Operation " << (int)pExpr->op << std::endl;
  arrow::compute::CompareOperator comparatorOperator;
  FilterOperator connective;
  std::shared_ptr<PredicateTree> predicate_tree = nullptr;

  switch (pExpr->op) {
    case TK_INTEGER:
    case TK_STRING:
    case TK_OR:
    case TK_AND: {
      Expr* leftExpr = pExpr->pLeft;
      Expr* rightExpr = pExpr->pRight;

      std::shared_ptr<PredicateTree> lpred_tree = ResolvePredExpr(leftExpr);
      std::shared_ptr<PredicateTree> rpred_tree = ResolvePredExpr(rightExpr);
      if (lpred_tree == nullptr || rpred_tree == nullptr) break;
      if (leftExpr->pLeft->iTable == rightExpr->pLeft->iTable) {
        if (pExpr->op == TK_OR) {
          std::shared_ptr<ConnectiveNode> connective_node =
              std::make_shared<ConnectiveNode>(lpred_tree.get()->root_,
                                               rpred_tree.get()->root_,
                                               FilterOperator::OR);
          predicate_tree = std::make_shared<PredicateTree>(connective_node);
        }
        if (pExpr->op == TK_AND) {
          std::shared_ptr<ConnectiveNode> connective_node =
              std::make_shared<ConnectiveNode>(lpred_tree.get()->root_,
                                               rpred_tree.get()->root_,
                                               FilterOperator::AND);
          predicate_tree = std::make_shared<PredicateTree>(connective_node);
        }

        select_predicates_[leftExpr->pLeft->y.pTab->zName] = predicate_tree;
      }
      break;
    }
    case TK_NE:
    case TK_EQ:
    case TK_GT:
    case TK_LE:
    case TK_LT:
    case TK_GE: {
      Expr* leftExpr = pExpr->pLeft;
      Expr* rightExpr = pExpr->pRight;

      ColumnReference colRef;
      arrow::Datum datum;

      if ((leftExpr != NULL && leftExpr->op == TK_COLUMN) &&
          (rightExpr != NULL &&
           (rightExpr->op == TK_INTEGER || rightExpr->op == TK_STRING))) {
        colRef = {catalog_->getTable(leftExpr->iTable),
                  leftExpr->y.pTab->aCol[leftExpr->iColumn].zName};
        if (rightExpr->op == TK_STRING) {
          datum = arrow::Datum(
              std::make_shared<arrow::StringScalar>(rightExpr->u.zToken));
        } else if (rightExpr->op == TK_INTEGER) {
          datum = arrow::Datum((uint32_t)rightExpr->u.iValue);
        }
        if (pExpr->op == TK_NE)
          comparatorOperator = arrow::compute::CompareOperator::NOT_EQUAL;
        if (pExpr->op == TK_EQ)
          comparatorOperator = arrow::compute::CompareOperator::EQUAL;
        if (pExpr->op == TK_LT)
          comparatorOperator = arrow::compute::CompareOperator::LESS;
        if (pExpr->op == TK_LE)
          comparatorOperator = arrow::compute::CompareOperator::LESS_EQUAL;
        if (pExpr->op == TK_GT)
          comparatorOperator = arrow::compute::CompareOperator::GREATER;
        if (pExpr->op == TK_GE)
          comparatorOperator = arrow::compute::CompareOperator::GREATER_EQUAL;

        Predicate predicate = {colRef, comparatorOperator, datum};
        auto predicate_node = std::make_shared<PredicateNode>(
            std::make_shared<Predicate>(predicate));
        predicate_tree = std::make_shared<PredicateTree>(predicate_node);

        select_predicates_[leftExpr->y.pTab->zName] = predicate_tree;
      }
      break;
    }
  }
  return predicate_tree;
}

bool SelectResolver::ResolveSelectTree(Select* queryTree) {
  // Collect all the src tables
  SrcList* pTabList = queryTree->pSrc;
  for (int i = 0; i < pTabList->nSrc; i++) {
    select_predicates_.insert({pTabList->a[i].zName, nullptr});
  }

  ExprList* pEList = queryTree->pEList;
  for (int k = 0; k < pEList->nExpr; k++) {
    ResolvePredExpr(pEList->a[k].pExpr);
  }

  for (int k = 0; k < pEList->nExpr; k++) {
    if (pEList->a[k].pExpr->op == TK_AGG_FUNCTION) {
      Expr* expr = pEList->a[k].pExpr->x.pList->a[0].pExpr;
      if (expr->iColumn > 0) {
        ColumnReference colRef = {catalog_->getTable(expr->iTable),
                                  expr->y.pTab->aCol[expr->iColumn].zName};
        char* zName = NULL;
        if (pEList->a[k].zEName != NULL) {
          zName = pEList->a[k].zEName;
        }
        AggregateReference aggRef = {
            aggregate_kernels_[pEList->a[k].pExpr->u.zToken],
            pEList->a[k].zEName, colRef};
        agg_references_.emplace_back(aggRef);
        ProjectReference projRef = {colRef, zName};
        project_references_.emplace_back(projRef);
      }
    } else if (pEList->a[k].pExpr->op == TK_COLUMN ||
               pEList->a[k].pExpr->op == TK_AGG_COLUMN) {
      Expr* expr = pEList->a[k].pExpr;
      ColumnReference colRef = {catalog_->getTable(expr->iTable),
                                expr->y.pTab->aCol[expr->iColumn].zName};
      ProjectReference projRef = {colRef};
      project_references_.emplace_back(projRef);
    }
  }

  // Resolve predicates
  Expr* pWhere = queryTree->pWhere;
  if (pWhere != NULL) {
    ResolvePredExpr(pWhere);
  }
  if (pWhere != NULL) {
    ResolveJoinPredExpr(pWhere);
  }

  // Resolve GroupBy
  ExprList* pGroupBy = queryTree->pGroupBy;
  if (pGroupBy != NULL) {
    for (int i = 0; i < pGroupBy->nExpr; i++) {
      if (pGroupBy->a[i].pExpr->iColumn >= 1) {
        ColumnReference colRef = {
            catalog_->getTable(pGroupBy->a[i].pExpr->iTable),
            pGroupBy->a[i]
                .pExpr->y.pTab->aCol[pGroupBy->a[i].pExpr->iColumn]
                .zName};
        group_by_references_.emplace_back(colRef);
      }
    }
  }

  // Resolve OrderBy
  ExprList* pOrderBy = queryTree->pOrderBy;
  if (pOrderBy != NULL) {
    for (int i = 0; i < pOrderBy->nExpr; i++) {
      if (pOrderBy->a[i].u.x.iOrderByCol > 0) {
        if (pOrderBy->a[i].pExpr->iColumn >= 0) {
          ColumnReference colRef;
          if (pOrderBy->a[i].pExpr->op == TK_AGG_FUNCTION) {
            colRef = {
                nullptr,
                project_references_[pOrderBy->a[i].u.x.iOrderByCol].alias};
          } else {
            colRef = {catalog_->getTable(pOrderBy->a[i].pExpr->iTable),
                      pOrderBy->a[i]
                          .pExpr->y.pTab->aCol[pOrderBy->a[i].pExpr->iColumn]
                          .zName};
          }
          order_by_references_.emplace_back(colRef);
        }
      }
    }
  }

  std::cout << "project references: " << project_references_.size()
            << std::endl;
  std::cout << "aggregate references: " << agg_references_.size() << std::endl;
  std::cout << "select tables: " << select_predicates_.size() << std::endl;
  std::cout << "Join Predicates: " << join_predicates_.size() << std::endl;
  std::cout << "group_by references: " << group_by_references_.size()
            << std::endl;
  std::cout << "order_by references: " << order_by_references_.size()
            << std::endl;

  return true;
}
}  // namespace resolver
}  // namespace hustle