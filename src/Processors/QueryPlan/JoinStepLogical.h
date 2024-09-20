#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Interpreters/JoinInfo.h>

namespace DB
{

/** JoinStepLogical is a logical step for JOIN operation.
  * Doesn't contain any specific join algorithm or other execution details.
  * It's place holder for join operation with it's description that can be serialized.
  * Transformed to actual join step during plan optimization.
  */
class JoinStepLogical final : public IQueryPlanStep
{
public:
    JoinStepLogical(
        const DataStream & left_stream_,
        const DataStream & right_stream_,
        JoinInfo join_info_,
        JoinExpressionActions join_expression_actions_,
        Names required_output_columns_,
        ContextPtr context_);

    String getName() const override { return "JoinLogical"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    bool canUpdateInputStream() const override { return true; }

    bool allowPushDownToRight() const;

protected:
    void updateOutputStream() override;

    JoinExpressionActions join_expression_actions;
    JoinInfo join_info;

    Names required_output_columns;
    ContextPtr context;
};

}
