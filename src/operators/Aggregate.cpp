#include "Aggregate.h"

#include <utility>
#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <table/util.h>
#include <iostream>


namespace hustle {
namespace operators {

Aggregate::Aggregate(AggregateKernels aggregate_kernel,
                     std::string aggregate_column_name,
                     std::string group_by_column_name) {
    aggregate_kernel_ = aggregate_kernel;
    aggregate_column_name_ = std::move(aggregate_column_name);
    group_by_column_name_ = std::move(group_by_column_name);
}

std::shared_ptr<Table> Aggregate::run_operator
        (std::vector<std::shared_ptr<Table>> tables) {

    auto table = tables[0];
    std::shared_ptr<Table> out_table;

    if (group_by_column_name_.empty()) {
        out_table = run_operator_no_group_by(table);
    } else {
        out_table = run_operator_with_group_by(table);
    }

    return out_table;
}

std::shared_ptr<Table> Aggregate::run_operator_with_group_by(
        std::shared_ptr<Table> table) {

    arrow::Status status;

    std::shared_ptr<arrow::Schema> out_schema;
    std::shared_ptr<arrow::ArrayBuilder> aggregate_builder;

    // Create output schema
    switch (aggregate_kernel_) {
        // Returns a Datum of the same type INT64
        case SUM: {
            out_schema = arrow::schema(
                    {arrow::field(group_by_column_name_, arrow::utf8()),
                     arrow::field("aggregate", arrow::int64())});
            aggregate_builder = std::make_shared<arrow::Int64Builder>();
            break;
        }
        case MEAN: {
            out_schema = arrow::schema(
                    {arrow::field(group_by_column_name_, arrow::utf8()),
                     arrow::field("aggregate", arrow::float64())});
            aggregate_builder = std::make_shared<arrow::DoubleBuilder>();
            break;
        }
        case COUNT: {
            throw std::runtime_error("Count aggregate not supported.");
        }
    }

    auto out_table = std::make_shared<Table>("aggregate", out_schema,
                                             BLOCK_SIZE);

    // Leaf Aggregate operator will only have one field in its GROUP BY column
    auto unique_values = get_unique_values(table)->field(0);

    auto unique_values_casted = std::static_pointer_cast<arrow::StringArray>
            (unique_values);

    for (int i=0; i<unique_values->length(); i++) {

        arrow::compute::Datum value(
                std::make_shared<arrow::StringScalar>(
                unique_values_casted->GetString(i)));
        auto filter = get_filter(table, value);
        auto aggregate_col = table->get_column_by_name(aggregate_column_name_);
        auto aggregate = compute_aggregate(aggregate_col, filter);

        // Append each group aggregate to aggregate_builder.
        // TODO(nicholas): Can you condense this?
        switch (aggregate_kernel_) {
            // Returns a Datum of type INT64
            case SUM: {
                auto aggregate_builder_casted =
                        std::static_pointer_cast<arrow::Int64Builder>
                                (aggregate_builder);
                auto aggregate_casted =
                        std::static_pointer_cast<arrow::Int64Scalar>
                                (aggregate.scalar());
                status = aggregate_builder_casted->Append(
                        aggregate_casted->value);
                evaluate_status(status, __FUNCTION__, __LINE__);
                break;
            }
            case COUNT: {
                throw std::runtime_error("Count aggregate not supported.");
                break;
            }
                // NOTE: Mean outputs a DOUBLE
            case MEAN: {
                auto aggregate_builder_casted =
                        std::static_pointer_cast<arrow::DoubleBuilder>
                                (aggregate_builder);
                auto aggregate_casted =
                        std::static_pointer_cast<arrow::DoubleScalar>
                                (aggregate.scalar());
                status = aggregate_builder_casted->Append(
                        aggregate_casted->value);
                evaluate_status(status, __FUNCTION__, __LINE__);
                break;
            }
        }
    }

    std::shared_ptr<arrow::Array> out_aggregate_array;
    std::shared_ptr<arrow::Array> out_group_by_array;

    status = aggregate_builder->Finish(&out_aggregate_array);
    evaluate_status(status, __FUNCTION__, __LINE__);

    out_table->insert_records(
            {unique_values->data(), out_aggregate_array->data()});
    return out_table;

}

std::shared_ptr<Table> Aggregate::run_operator_no_group_by
(std::shared_ptr<Table> table) {

    arrow::Status status;

    std::shared_ptr<arrow::Schema> out_schema;

    // TODO(nicholas): output schema construction would be much simpler if we
    //  passed in fields instead of column names into the Aggregate constructor
    switch (aggregate_kernel_) {
        // Returns a Datum of the same type INT64
        case SUM: {
            out_schema = arrow::schema({arrow::field("aggregate",
                                                     arrow::int64())});
            break;
        }
        case COUNT: {
            throw std::runtime_error("Count aggregate not supported.");
        }
            // NOTE: Mean outputs a DOUBLE
        case MEAN: {
            out_schema = arrow::schema({arrow::field("aggregate",
                                                     arrow::float64())});
            break;
        }
    }

    auto aggregate_col = table->get_column_by_name(aggregate_column_name_);
    auto aggregate = compute_aggregate(aggregate_col, nullptr);


    std::shared_ptr<arrow::Array> out_array;
    std::shared_ptr<arrow::ArrayData> out_data;
    status = arrow::MakeArrayFromScalar(
            arrow::default_memory_pool(),
            *aggregate.scalar(),
            1,
            &out_array);
    evaluate_status(status, __FUNCTION__, __LINE__);

    auto out_table = std::make_shared<Table>("aggregate", out_schema,
            BLOCK_SIZE);
    out_table->insert_records({out_array->data()});

    return out_table;
}

std::shared_ptr<arrow::StructArray> Aggregate::get_unique_values
    (std::shared_ptr<Table> table) {

    arrow::Status status;

    arrow::compute::FunctionContext function_context(
            arrow::default_memory_pool());
    arrow::compute::TakeOptions take_options;
    std::shared_ptr<arrow::Array> unique_values;

    auto group_by_col = table->get_column_by_name(group_by_column_name_);

    status = arrow::compute::Unique(&function_context, group_by_col,
                                    &unique_values);
    evaluate_status(status, __FUNCTION__, __LINE__);

    auto result = arrow::StructArray::Make({unique_values},{group_by_column_name_});
    evaluate_status(result.status(), __FUNCTION__, __LINE__);

    return result.ValueOrDie();
}

std::shared_ptr<arrow::ChunkedArray> Aggregate::get_filter
(std::shared_ptr<Table> table, arrow::compute::Datum value) {

    arrow::Status status;

    arrow::compute::FunctionContext function_context(
            arrow::default_memory_pool());
    arrow::compute::CompareOptions compare_options(
            arrow::compute::CompareOperator::EQUAL);
    arrow::compute::Datum filter;
    arrow::ArrayVector filter_vector;

    for (int block_id=0; block_id<table->get_num_blocks(); block_id++) {
        auto block_col = table->get_block(block_id)->get_column_by_name
                (group_by_column_name_);

        // Note that Compare does not operate on ChunkedArrays, so we must
        // compute the filter block by block and combine them into a
        // ChunkedArray.
        status = arrow::compute::Compare(&function_context, block_col, value,
                compare_options, &filter);
        evaluate_status(status, __FUNCTION__, __LINE__);

        filter_vector.push_back(filter.make_array());
    }

    return std::make_shared<arrow::ChunkedArray>(filter_vector);
}


arrow::compute::Datum Aggregate::compute_aggregate(
        std::shared_ptr<arrow::ChunkedArray> aggregate_col,
        std::shared_ptr<arrow::ChunkedArray> group_filter) {

    arrow::Status status;

    arrow::compute::FunctionContext function_context(
            arrow::default_memory_pool());
    std::shared_ptr<arrow::ChunkedArray> aggregate_group_col;

    if (group_filter == nullptr) {
        aggregate_group_col = aggregate_col;
    } else {
        status = arrow::compute::Filter(
                &function_context,
                *aggregate_col,
                *group_filter,
                &aggregate_group_col);
        evaluate_status(status, __FUNCTION__, __LINE__);
    }

    arrow::compute::Datum out_aggregate;

    switch (aggregate_kernel_) {
        // Returns a Datum of the same type INT64
        case SUM: {
            status = arrow::compute::Sum(
                    &function_context,
                    aggregate_group_col,
                    &out_aggregate
            );
            break;
        }
            // Returns a Datum of the same type as the column
        case COUNT: {
            // TODO(martin): count options
            // TODO(nicholas): Currently, Count cannot accept
            //  ChunkedArray Datums. Support for ChunkedArray Datums
            //  was recently added (late January) for Sum and Mean,
            //  and it seems reasonable to assume that it will be
            //  implemented for Count soon too. Once support is
            //  added, we can remove the line below.
            throw std::runtime_error("Count aggregate not supported.");
            auto count_options = arrow::compute::CountOptions(
                    arrow::compute::CountOptions::COUNT_ALL);
            status = arrow::compute::Count(
                    &function_context,
                    count_options,
                    aggregate_group_col,
                    &out_aggregate
            );
            evaluate_status(status, __FUNCTION__, __LINE__);
            break;
        }
            // NOTE: Mean outputs a DOUBLE
        case MEAN: {
            status = arrow::compute::Mean(
                    &function_context,
                    aggregate_group_col,
                    &out_aggregate
            );
            break;
        }
    }
    evaluate_status(status, __FUNCTION__, __LINE__);
    return out_aggregate;
}

AggregateComposite::AggregateComposite(
            std::shared_ptr<AggregateOperator> left_child,
            std::shared_ptr<AggregateOperator> right_child) {

    left_child_ = left_child;
    right_child_ = right_child;
}

//    for (int i=0; i<left_unique_values->length(); i++) {
//    for (int j=0; j<right_unique_values->length(); j++) {
//
//
//}
//}

std::shared_ptr<arrow::StructArray> AggregateComposite::get_unique_values(
        std::shared_ptr<Table> table) {

    arrow::Status status;
    auto left_unique_values = left_child_->get_unique_values(table);
    auto right_unique_values = right_child_->get_unique_values(table);

    auto result = arrow::StructArray::Make({left_unique_values,
                                        right_unique_values},
            {arrow::field("left",arrow::utf8()),
             arrow::field("right",arrow::utf8())});

    status = result.status();
    evaluate_status(status, __FUNCTION__, __LINE__);

    return result.ValueOrDie();
}

std::shared_ptr<arrow::ChunkedArray> AggregateComposite::get_filter(
        std::shared_ptr<Table> table, arrow::compute::Datum value) {

    arrow::Status status;

    auto value_struct_scalar = std::make_shared<arrow::StructScalar>(
            value.value);
    auto value_scalars = value_struct_scalar->value;
    arrow::compute::Datum left_value(value_scalars[0]);
    arrow::compute::Datum right_value(value_scalars[1]);
    
    auto left_filter = left_child_->get_filter(table, left_value);
    auto right_filter = right_child_->get_filter(table, right_value);

    arrow::compute::FunctionContext function_context(
            arrow::default_memory_pool());
    arrow::compute::CompareOptions compare_options(
            arrow::compute::CompareOperator::EQUAL);
    arrow::compute::Datum filter;

    arrow::ArrayVector filter_vector;

    // Note: left_filter and right_filter have equal number of chunks
    for (int i=0; i<left_filter->num_chunks(); i++) {
        status = arrow::compute::And(
                &function_context,
                left_filter->chunk(i),
                right_filter->chunk(i),
                &filter);
        evaluate_status(status, __FUNCTION__, __LINE__);
        filter_vector.push_back(filter.make_array());
    };

    return std::make_shared<arrow::ChunkedArray>(filter_vector);
}

} // namespace operators
} // namespace hustle
