package io.prestosql.operator.aggregation;

import io.airlift.slice.Slice;
import io.prestosql.operator.aggregation.state.NullableDoubleState;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.*;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.util.SliceUtils;

/**
 * @author jinhai
 * @date 2019/05/08
 */
@AggregationFunction("sum")
public final class VarcharSumAggregation
{
    private VarcharSumAggregation() {}

    @InputFunction
    public static void sum(@AggregationState NullableDoubleState state, @SqlType(StandardTypes.VARCHAR) Slice slice)
    {
        state.setNull(false);
        state.setDouble(state.getDouble() + SliceUtils.toDouble(slice));
    }

    @CombineFunction
    public static void combine(@AggregationState NullableDoubleState state, @AggregationState NullableDoubleState otherState)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setDouble(otherState.getDouble());
            return;
        }

        state.setDouble(state.getDouble() + otherState.getDouble());
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState NullableDoubleState state, BlockBuilder out)
    {
        NullableDoubleState.write(DoubleType.DOUBLE, state, out);
    }
}