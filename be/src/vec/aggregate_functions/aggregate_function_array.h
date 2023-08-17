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

namespace doris::vectorized {
    template<typename T>
    struct AggregateFunctionArrayAggData {
        using ColumnType = ColumnVector<T>;

        void add(const ColumnType& column,size_t offset,size_t count) { data_column.append(column, offset, count); }

        ColumnType data_column; // Aggregated elements for array_agg
    };

/** Not an aggregate function, but an adapter of aggregate functions,
  *  which any aggregate function `agg(x)` makes an aggregate function of the form `array_agg(x)`.
  * The adapted aggregate function calculates nested aggregate function for each element of the array.
  */
    template<typename Data, typename T>
    class AggregateFunctionArrayAgg final :
            public IAggregateFunctionHelper<Data, AggregateFunctionArrayAgg<Data,T>> {
    private:

    public:
        using InputColumnType = ColumnVector<T>;

        AggregateFunctionArrayAgg() = default;

        AggregateFunctionArrayAgg(const DataTypes& argument_types_) : IAggregateFunctionHelper<Data,
                AggregateFunctionArrayAgg<Data, T>>(argument_types_) {}

        std::string get_name() const override {
            return "array_agg";
        }

        void create(AggregateDataPtr __restrict place) const override {
            new (place) Data(argument_types_);
        }

        void
        add(AggregateDataPtr __restrict place, const IColumn **columns, size_t row_num, Arena *arena) const override {
            const auto& column = down_cast<const InputColumnType&>(*columns[0]);
            this->data(place)(column, row_num, 1);
        }


    }

};