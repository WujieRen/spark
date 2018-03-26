package com.rwj.offlineAnalysisPrj.spark.product;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.datanucleus.store.types.backed.ArrayList;

import java.util.Arrays;

/**
 * 组内拼接去重函数
 * Created by renwujie on 2018/03/21 at 10:48
 */
public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {

    private StructType inputSchema = DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("cityInfo", DataTypes.StringType, true)));
    private StructType bufferSchema = DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("bufferedCityInfo", DataTypes.StringType, true)));
    private DataType dataType = DataTypes.StringType;
    private boolean deterministic = true;

    @Override
    public StructType inputSchema() {
        return inputSchema;
    }

    @Override
    public StructType bufferSchema() {
        return bufferSchema;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public boolean deterministic() {
        return deterministic;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, "");
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        String bufferCityInfo = buffer.getString(0);
        String cityInfo = input.getString(0);

        if(!bufferCityInfo.contains(cityInfo)) {
            if("".equals(cityInfo)) {
                bufferCityInfo += cityInfo;
            } else {
                bufferCityInfo += "," + cityInfo;
            }

            buffer.update(0, bufferCityInfo);
        }
    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        String bufferCityInfo1 = buffer1.getString(0);
        String bufferCityInfo2 = buffer2.getString(0);

        for(String cityInfo : bufferCityInfo2.split(",")) {
            if(!bufferCityInfo1.contains(cityInfo)) {
                if("".equals(bufferCityInfo1)) {
                    bufferCityInfo1 += cityInfo;
                } else {
                    bufferCityInfo1 += "," + cityInfo;
                }
            }
        }

        buffer1.update(0, bufferCityInfo1);
    }

    @Override
    public Object evaluate(Row buffer) {
        return buffer.getString(0);
    }
}
