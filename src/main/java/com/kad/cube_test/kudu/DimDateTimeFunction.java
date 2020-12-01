package com.kad.cube_test.kudu;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.stream.Stream;

public class DimDateTimeFunction extends TableFunction<Row> {

    private static final long serialVersionUID = 401230307848777998L;
    private static final String DEFAULT_FIELD_PREFIX = "datetime";
    private static final TableSchema DIM_DATA_TIME_TABLESCHEMA = TableSchema.builder()
            .field("datetime", DataTypes.TIMESTAMP(3))
            .field("date", DataTypes.TIMESTAMP(3))
            .field("year", DataTypes.INT())
            .field("month", DataTypes.INT())
            .field("day", DataTypes.INT())
            .field("quarter", DataTypes.INT())
            .field("day_of_week", DataTypes.INT())
            .field("week_start_date", DataTypes.TIMESTAMP(3))
            .field("hour", DataTypes.INT())
            .build();


    private final DataType outputType;

    public DimDateTimeFunction() {
        this(DEFAULT_FIELD_PREFIX);
    }

    public DimDateTimeFunction(String fieldPrefix) {
        DataTypes.Field[] fields = Stream.of(DIM_DATA_TIME_TABLESCHEMA.getFieldNames())
                .map(name -> DataTypes.FIELD(
                        String.format("%s_%s", fieldPrefix, name),
                        DIM_DATA_TIME_TABLESCHEMA.getFieldDataType(name).get())
                )
                .toArray(DataTypes.Field[]::new);
        this.outputType = DataTypes.ROW(fields);
    }

    public void eval(LocalDateTime dateTime) {
        LocalDate localDate = dateTime.toLocalDate();
        int dayOfWeek = localDate.getDayOfWeek().getValue();
        collect(Row.of(
                dateTime,
                localDate.atTime(0, 0), // date
                dateTime.getYear(),  // year
                dateTime.getMonthValue(),  // month
                dateTime.getDayOfMonth(),   // day
                (dateTime.getMonthValue() + 2) / 3, // quarter 季度
                dayOfWeek,  // day_of_week 周几
                localDate.minusDays(dayOfWeek - 1).atTime(0, 0),  // week_start_date:这周的起始日期
                dateTime.getHour()  // hour
        ));
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                // 指定输入参数的类型，必要时参数会被隐式转换
                .typedArguments(DataTypes.TIMESTAMP(3))
                // specify a strategy for the result data type of the function
                .outputTypeStrategy(callContext -> {
                    return Optional.of(this.outputType);
                }).build();
    }
}
