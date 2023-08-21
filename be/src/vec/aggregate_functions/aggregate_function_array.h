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
        _type = remove_nullable(argument_types[0]);
        _column = _type->create_column();
    }

    void add(const StringRef& column) {
        ArenaKeyHolder key_holder {column, _arena};
        if (column.size > 0) {
            key_holder_persist_key(key_holder);
        }
        _column->insert_data(key_holder.key.data, key_holder.key.size);
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

        size_t num_rows = _column->size();
        for(size_t i=0;i<num_rows;++i){

            auto column=static_cast<ColumnType&>(*_column).get_data_at(i);

            assert_cast<ColumnType&>(to).insert_data(column.data,column.size);
        }
    }

private:
    using ColumnType =
            std::conditional_t<std::is_same_v<String, K>, ColumnString, ColumnVectorOrDecimal<K>>;
    IColumn::MutablePtr _column;
    DataTypePtr _type;
    Arena _arena;
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
        if(columns[0]->is_nullable()) {
            auto& nullable_col = assert_cast<const ColumnNullable&>(*columns[0]);
            auto& nullable_map = nullable_col.get_null_map_data();
            if (nullable_map[row_num]) {
                //todo
                return;
            }
            this->data(place).add(
                    assert_cast<const ColumnType&>(nullable_col.get_nested_column())
                            .get_data_at(row_num));
        } else {
            this->data(place).add(
                    assert_cast<const ColumnType&>(*columns[0]).get_data_at(row_num));
        }
    }

    void streaming_agg_serialize_to_column(const IColumn** columns, MutableColumnPtr& dst,
                                           const size_t num_rows, Arena* arena) const override {
        //todo
    }

    void deserialize_from_column(AggregateDataPtr places, const IColumn& column, Arena* arena,
                                 size_t num_rows) const override {
        //todo
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override {
        //todo
    }

    void deserialize_and_merge_from_column(AggregateDataPtr __restrict place, const IColumn& column,
                                           Arena* arena) const override {
        //auto& column_arr = assert_cast<const ColumnArray&>(column);
        const size_t num_rows = column.size();
        for (size_t i = 0; i != num_rows; ++i){
            this->data(place).add(column[i]);
        }
      //  auto& column_arr = assert_cast<const ColumnArray&>(column);
       // auto& column_nested_col = column_arr.get_data();
     /*   const size_t num_rows = column.size();
        for (size_t i = 0; i != num_rows; ++i){

            StringRef key;
            if constexpr (std::is_same_v<T, String>){
                auto string = this->data(place).add(
                        column[i].get<T>());
                key = string;
            }else{
                auto& k = column[i].get<Type>();
                key.data = reinterpret_cast<const char*>(&k);
                key.size = sizeof(k);
            }*/


           /* if(column_nested_col.is_nullable()){
                auto& nullable_col = assert_cast<const ColumnNullable&>(column);
                auto& nullable_map = nullable_col.get_null_map_data();
                if (nullable_map[i]) {
                    //todo
                    return;
                }
                this->data(place).add(
                        assert_cast<const ColumnType&>(nullable_col.get_nested_column())
                                .get_data_at(i));
            }else{
                this->data(place).add(
                        assert_cast<const ColumnType&>(column).get_data_at(i));
            }*/
        //}



    }

    void deserialize_and_merge_from_column_range(AggregateDataPtr __restrict place,
                                                 const IColumn& column, size_t begin, size_t end,
                                                 Arena* arena) const override {
        //todo
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const ColumnString* column, Arena* arena,
                                   const size_t num_rows) const override {
        //todo
    }

    void deserialize_and_merge_vec_selected(const AggregateDataPtr* places, size_t offset,
                                            AggregateDataPtr rhs, const ColumnString* column,
                                            Arena* arena, const size_t num_rows) const override {
        //todo
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to) const override {
        auto& to_arr = assert_cast<ColumnArray&>(to);
        auto& to_nested_col = to_arr.get_data();
        if (to_nested_col.is_nullable()) {
            auto col_null = reinterpret_cast<ColumnNullable*>(&to_nested_col);
            this->data(place).insert_result_into(col_null->get_nested_column());
            col_null->get_null_map_data().resize_fill(col_null->get_nested_column().size(), 0);
        } else {
            this->data(place).insert_result_into(to_nested_col);
        }
        to_arr.get_offsets().push_back(to_nested_col.size());
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& to_arr = assert_cast<ColumnArray&>(to);
        auto& to_nested_col = to_arr.get_data();
        if (to_nested_col.is_nullable()) {
            auto col_null = reinterpret_cast<ColumnNullable*>(&to_nested_col);
            this->data(place).insert_result_into(col_null->get_nested_column());
            col_null->get_null_map_data().resize_fill(col_null->get_nested_column().size(), 0);
        } else {
            this->data(place).insert_result_into(to_nested_col);
        }
        to_arr.get_offsets().push_back(to_nested_col.size());
    }

    [[nodiscard]] MutableColumnPtr create_serialize_column() const override {
        return get_return_type()->create_column();
    }

    [[nodiscard]] DataTypePtr get_serialized_type() const override { return get_return_type(); }

protected:
    using IAggregateFunction::argument_types;
};

} // namespace doris::vectorized
