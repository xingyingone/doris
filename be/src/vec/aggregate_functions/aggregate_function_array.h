// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <string.h>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_array.h"
#include "vec/data_types/data_type_array.h"
#include "vec/common/assert_cast.h"
#include "vec/common/hash_table/hash_table_key_holder.h"
namespace doris::vectorized {
template <typename K>
struct AggregateFunctionArrayAggData {
    using Type = std::conditional_t<std::is_same_v<K, String>, StringRef, K>;
    AggregateFunctionArrayAggData() { __builtin_unreachable(); }

    AggregateFunctionArrayAggData(const DataTypes& argument_types) {
        _type = make_nullable(argument_types[0]);
        _column = _type->create_column();
    }

    void add(const StringRef& column) {
        _column->insert_data(column.data, column.size);
    }

    void add(const Field& column){
        auto column_array = vectorized::get<Array>(column);
        const auto count = column_array.size();

        for (size_t i = 0; i != count; ++i){
           /* StringRef key;
            if constexpr (std::is_same_v<K, String>) {
                auto string = column_array[i].get<K>();
                key = string;
            } else {
                auto& k = column_array[i].get<Type>();
                key.data = reinterpret_cast<const char*>(&k);
                key.size = sizeof(k);
            }*/


            _column->insert(column_array[i]);
        }

    }

    void reset() {
        _column->clear();
    }

    void insert_result_into(IColumn& to) const {
        auto& to_arr = assert_cast<ColumnArray&>(to);
        auto& to_nested_col = to_arr.get_data();
        size_t num_rows = _column->size();
        if (to_nested_col.is_nullable()) {
            auto col_null = reinterpret_cast<ColumnNullable*>(&to_nested_col);
            for(size_t i=0;i<num_rows;++i){
                auto column=_column->get_data_at(i);
                assert_cast<ColumnType&>(col_null->get_nested_column()).insert_data(column.data,column.size);
                if(column.data==NULL||column.size==0){
                    col_null->get_null_map_data().push_back(1);
                }else{
                    col_null->get_null_map_data().push_back(0);
                }
            }
        }else{
            for(size_t i=0;i<num_rows;++i){

                auto column=_column->get_data_at(i);

                assert_cast<ColumnType&>(to_nested_col).insert_data(column.data,column.size);
            }
        }
        to_arr.get_offsets().push_back(to_nested_col.size());
    }


private:
    using ColumnType =
            std::conditional_t<std::is_same_v<String, K>, ColumnString, ColumnVectorOrDecimal<K>>;
    IColumn::MutablePtr _column;
    DataTypePtr _type;
};

/** Not an aggregate function, but an adapter of aggregate functions,
  *  which any aggregate function `agg(x)` makes an aggregate function of the form `array_agg(x)`.
  * The adapted aggregate function calculates nested aggregate function for each element of the array.
  */
template <typename Data, typename T>
class AggregateFunctionArrayAgg
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionArrayAgg<Data, T>> {
public:
    using ColumnType =
            std::conditional_t<std::is_same_v<String, T>, ColumnString, ColumnVectorOrDecimal<T>>;
    AggregateFunctionArrayAgg() = default;

    AggregateFunctionArrayAgg(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionArrayAgg<Data, T>>(
                      argument_types_) {}

    std::string get_name() const override { return "array_agg"; }

    void create(AggregateDataPtr __restrict place) const override {
        new (place) Data(argument_types);
    }

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeArray>(make_nullable(argument_types[0]));
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena* arena) const override {
    }

    void serialize(ConstAggregateDataPtr /* __restrict place */,
                   BufferWritable& /* buf */) const override {
        __builtin_unreachable();
    }

    void deserialize(AggregateDataPtr /* __restrict place */, BufferReadable& /* buf */,
                     Arena*) const override {
        __builtin_unreachable();
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena* arena) const override {
        this->data(place).add(
                columns[0]->get_data_at(row_num));
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena* arena) const override {
        __builtin_unreachable();
    }

    void deserialize_from_column(AggregateDataPtr places, const IColumn& column, Arena* arena,
                                 size_t num_rows) const override {
        __builtin_unreachable();
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        for (size_t i = 0; i != num_rows; ++i){
            this->data(places[i] + offset).insert_result_into(*dst);
        }
    }

    void deserialize_and_merge_from_column(AggregateDataPtr __restrict place, const IColumn& column,
                                           Arena* arena) const override {
        const size_t num_rows = column.size();
        for (size_t i = 0; i != num_rows; ++i){
            this->data(place).add(column[i]);
        }
    }

    void deserialize_and_merge_from_column_range(AggregateDataPtr __restrict place,
                                                 const IColumn& column, size_t begin, size_t end,
                                                 Arena* arena) const override {
        __builtin_unreachable();
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const ColumnString* column, Arena* arena,
                                   const size_t num_rows) const override {
        auto& col = assert_cast<const ColumnArray&>(*assert_cast<const IColumn*>(column));
        for (size_t i = 0; i != num_rows; ++i){
            this->data(places[i]).add(col[i]);
        }
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const ColumnString* column,
                                            Arena* arena, const size_t num_rows) const override {
        auto& col = assert_cast<const ColumnArray&>(*assert_cast<const IColumn*>(column));
        for (size_t i = 0; i != num_rows; ++i){
            if (places[i]){
                this->data(places[i]).add(col[i]);
            }
        }
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }


    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

    [[nodiscard]] MutableColumnPtr create_serialize_column() const override {
        return get_return_type()->create_column();
    }

    [[nodiscard]] DataTypePtr get_serialized_type() const override { return get_return_type(); }

protected:
    using IAggregateFunction::argument_types;
};

} // namespace doris::vectorized
