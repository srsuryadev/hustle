#ifndef HUSTLE_SRC_PARSER_PARSER_H_
#define HUSTLE_SRC_PARSER_PARSER_H_

#include "api/HustleDB.h"
#include "ParseTree.h"

extern const int SERIAL_BLOCK_SIZE = 4096;
char project[SERIAL_BLOCK_SIZE];
char loopPred[SERIAL_BLOCK_SIZE];
char otherPred[SERIAL_BLOCK_SIZE];
char groupBy[SERIAL_BLOCK_SIZE];
char orderBy[SERIAL_BLOCK_SIZE];
char* currPos = nullptr;


namespace hustle {
namespace parser {

class Parser {
 public:
  void parse(const std::string& sql, hustle::HustleDB& hustleDB) {
    std::cout << "For query: " << sql << std::endl <<
              "The plan is: " << std::endl <<
              hustleDB.getPlan(sql) << std::endl;

    std::string text =
        "{\"project\": [" + std::string(project) +
        "], \"loop_pred\": [" + std::string(loopPred) +
        "], \"other_pred\": [" + std::string(otherPred) +
        "], \"group_by\": [" + std::string(groupBy) +
        "], \"order_by\": [" + std::string(orderBy) +
        "]}";

    json j = json::parse(text);
    parse_tree_ = j;
    preprocessing();
  }

  std::shared_ptr<ParseTree> get_parse_tree() {
    return parse_tree_;
  }

  /**
   * Move select predicates from loop_pred to other_pred
   */
  void preprocessing() {
    for (auto& loop_pred : parse_tree_->loop_pred) {
      for (auto it = loop_pred->predicates.begin(); it != loop_pred->predicates.end(); ) {
        if ((*it)->plan_type == "SELECT_Pred") {
          parse_tree_->other_pred.push_back(std::move(*it));
          loop_pred->predicates.erase(it);
        } else {
          it += 1;
        }
      }
    }
  }

  /**
   * Dump the parse tree
   * @param indent
   */
  std::string to_string(int indent) {
    json j = parse_tree_;
    return j.dump(indent);
  }

 private:
  std::shared_ptr<ParseTree> parse_tree_;
};

}
}
#endif //HUSTLE_SRC_PARSER_PARSER_H_
