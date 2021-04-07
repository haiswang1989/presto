/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.operator.scalar;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.ParameterizedType;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.newArray;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.USE_BOXED_TYPE;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.util.CompilerUtils.defineClass;
import static io.prestosql.util.CompilerUtils.makeClassName;
import static io.prestosql.util.Failures.checkCondition;
import static io.prestosql.util.Reflection.methodHandle;
import static java.lang.Math.addExact;
import static java.util.Collections.nCopies;

public final class ConcatWsFunction
        extends SqlScalarFunction
{
    public static final ConcatWsFunction VARCHAR_CONCAT_WS = new ConcatWsFunction(createUnboundedVarcharType().getTypeSignature(), "concatenates given strings");

    public static final ConcatWsFunction VARBINARY_CONCAT_WS = new ConcatWsFunction(VARBINARY.getTypeSignature(), "concatenates given varbinary values");

    private final String description;

    private ConcatWsFunction(TypeSignature type, String description)
    {
        super(new Signature(
                "concat_ws",
                FunctionKind.SCALAR,
                ImmutableList.of(),
                ImmutableList.of(),
                type,
                ImmutableList.of(type),
                true));
        this.description = description;
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return description;
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        if (arity < 2) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "There must be two or more concatenation arguments");
        }

        Class<?> clazz = generateConcatWs(getSignature().getReturnType(), arity);
        MethodHandle methodHandle = methodHandle(clazz, "concatWs", nCopies(arity, Slice.class).toArray(new Class<?>[arity]));

        List<ScalarFunctionImplementation.ArgumentProperty> argumentProperties = new ArrayList<>(arity);
        argumentProperties.add(valueTypeArgumentProperty(RETURN_NULL_ON_NULL));
        argumentProperties.addAll(nCopies(arity - 1, valueTypeArgumentProperty(USE_BOXED_TYPE)));

        return new ScalarFunctionImplementation(
                false,
                argumentProperties,
                methodHandle,
                isDeterministic());
    }

    private static Class<?> generateConcatWs(TypeSignature type, int arity)
    {
        checkCondition(arity <= 254, NOT_SUPPORTED, "Too many arguments for string concatenation");
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(type.getBase() + "_concatWs" + arity + "ScalarFunction"),
                type(Object.class));

        // Generate constructor
        definition.declareDefaultConstructor(a(PRIVATE));

        // Generate concatWs()
        List<Parameter> parameters = IntStream.range(0, arity)
                .mapToObj(i -> arg("arg" + i, Slice.class))
                .collect(toImmutableList());

        MethodDefinition method = definition.declareMethod(a(PUBLIC, STATIC), "concatWs", type(Slice.class), parameters);
        BytecodeBlock body = method.getBody();
        body.append(generateConcatWsCode(constantInt(arity), newArray(ParameterizedType.type(Slice[].class), parameters))).retObject();

        return defineClass(definition, Object.class, ImmutableMap.of(), new DynamicClassLoader(ConcatWsFunction.class.getClassLoader()));
    }

    private static BytecodeExpression generateConcatWsCode(BytecodeExpression x, BytecodeExpression y)
    {
        return invokeStatic(ConcatWsFunction.class, "concatWsCode", Slice.class, x, y);
    }

    @UsedByGeneratedCode
    public static Slice concatWsCode(int arity, Slice[] slices)
    {
        if (arity == 2) {
            if (slices[1] == null) {
                return Slices.EMPTY_SLICE;
            }
            return Slices.copyOf(slices[1]);
        }

        List<String> slicesSkipNull = new ArrayList<>(arity);
        int sum = 0;
        for (int i = 1; i < arity; i++) {
            if (slices[i] != null) {
                String value = slices[i].toStringUtf8();
                sum = checkedAdd(sum, value.length());
                slicesSkipNull.add(value);
            }
        }

        String result = Joiner.on(slices[0].toStringUtf8()).join(slicesSkipNull);
        return Slices.utf8Slice(result);
    }

    private static int checkedAdd(int x, int y)
    {
        try {
            return addExact(x, y);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Concatenated string is too large");
        }
    }
}
