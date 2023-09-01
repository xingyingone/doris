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

#include <assert.h>
#include <glog/logging.h>
#include <string.h>

#include <limits>
#include <memory>
#include <new>
#include <string>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/key_holder_helpers.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/hash_table/hash_set.h"
#include "vec/common/hash_table/hash_table_key_holder.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/io/io_helper.h"
#include "vec/io/var_int.h"

namespace doris {
namespace vectorized {
class Arena;
} // namespace vectorized
} // namespace doris
template <typename, typename>
struct DefaultHash;

namespace doris::vectorized {

template <typename T, typename HasLimit>
struct AggregateFunctionCollectSetData {
    using ElementType = T;
    using ColVecType = ColumnVectorOrDecimal<ElementType>;
    using ElementNativeType = typename NativeType<T>::Type;
    using SelfType = AggregateFunctionCollectSetData;
    using Set = HashSetWithStackMemory<ElementNativeType, DefaultHash<ElementNativeType>, 4>;
    Set data_set;
    Int64 max_size = -1;

    size_t size() const { return data_set.size(); }

    void add(const IColumn& column, size_t row_num) {
        data_set.insert(assert_cast<const ColVecType&>(column).get_data()[row_num]);
    }

    void merge(const SelfType& rhs) {
        if constexpr (HasLimit::value) {
            DCHECK(max_size == -1 || max_size == rhs.max_size);
            max_size = rhs.max_size;

            for (auto& rhs_elem : rhs.data_set) {
                if (size() >= max_size) {
                    return;
                }
                data_set.insert(rhs_elem.get_value());
            }
        } else {
            data_set.merge(rhs.data_set);
        }
    }

    void write(BufferWritable& buf) const {
        data_set.write(buf);
        write_var_int(max_size, buf);
    }

    void read(BufferReadable& buf) {
        data_set.read(buf);
        read_var_int(max_size, buf);
    }

    void insert_result_into(IColumn& to) const {
        auto& vec = assert_cast<ColVecType&>(to).get_data();
        vec.reserve(size());
        for (const auto& item : data_set) {
            vec.push_back(item.key);
        }
    }

    void reset() { data_set.clear(); }
};

template <typename HasLimit>
struct AggregateFunctionCollectSetData<StringRef, HasLimit> {
    using ElementType = StringRef;
    using ColVecType = ColumnString;
    using SelfType = AggregateFunctionCollectSetData<ElementType, HasLimit>;
    using Set = HashSetWithSavedHashWithStackMemory<ElementType, DefaultHash<ElementType>, 4>;
    Set data_set;
    Int64 max_size = -1;

    size_t size() const { return data_set.size(); }

    void add(const IColumn& column, size_t row_num, Arena* arena) {
        Set::LookupResult it;
        bool inserted;
        auto key_holder = get_key_holder<true>(column, row_num, *arena);
        data_set.emplace(key_holder, it, inserted);
    }

    void merge(const SelfType& rhs, Arena* arena) {
        bool inserted;
        Set::LookupResult it;
        DCHECK(max_size == -1 || max_size == rhs.max_size);
        max_size = rhs.max_size;

        for (auto& rhs_elem : rhs.data_set) {
            if constexpr (HasLimit::value) {
                if (size() >= max_size) {
                    return;
                }
            }
            assert(arena != nullptr);
            data_set.emplace(ArenaKeyHolder {rhs_elem.get_value(), *arena}, it, inserted);
        }
    }

    void write(BufferWritable& buf) const {
        write_var_uint(size(), buf);
        for (const auto& elem : data_set) {
            write_string_binary(elem.get_value(), buf);
        }
        write_var_int(max_size, buf);
    }

    void read(BufferReadable& buf) {
        UInt64 size;
        read_var_uint(size, buf);
        StringRef ref;
        for (size_t i = 0; i < size; ++i) {
            read_string_binary(ref, buf);
            data_set.insert(ref);
        }
        read_var_int(max_size, buf);
    }

    void insert_result_into(IColumn& to) const {
        auto& vec = assert_cast<ColVecType&>(to);
        vec.reserve(size());
        for (const auto& item : data_set) {
            vec.insert_data(item.key.data, item.key.size);
        }
    }

    void reset() { data_set.clear(); }

};

template <typename T, typename HasLimit ,bool Nullable>
struct AggregateFunctionCollectListData{
    using ElementType = T;
    using ColVecType = ColumnVectorOrDecimal<ElementType>;
    using SelfType = AggregateFunctionCollectListData<ElementType, HasLimit,Nullable>;
    /**add by alex*/
    MutableColumnPtr column_data;
    ColVecType* nested_column;
    NullMap* null_map;
    /**end*/
    PaddedPODArray<ElementType> data;
    Int64 max_size = -1;

    size_t size() const { return data.size(); }

   AggregateFunctionCollectListData(){
        if constexpr (Nullable){
            if constexpr (IsDecimalNumber<T>){
                //column_data = ColumnNullable::create(ColVecType::create(0,0),ColumnUInt8::create());
                //null_map=&(assert_cast<ColumnNullable&>(*column_data).get_null_map_data());
                //nested_column= assert_cast<ColVecType*>(assert_cast<ColumnNullable&>(*column_data).get_nested_column_ptr().get());
            }else{
                column_data = ColumnNullable::create(ColVecType::create(),ColumnUInt8::create());
                null_map=&(assert_cast<ColumnNullable&>(*column_data).get_null_map_data());
                nested_column= assert_cast<ColVecType*>(assert_cast<ColumnNullable&>(*column_data).get_nested_column_ptr().get());
            }
        }
    }


    void add(const IColumn& column, size_t row_num) {
        if constexpr (Nullable){
            DCHECK(null_map->size()==nested_column->size());
            const auto& col= assert_cast<const ColumnNullable&>(column);
            const auto& vec=assert_cast<const ColVecType&>(col.get_nested_column()).get_data();
            null_map->push_back(col.get_null_map_data()[row_num]);
            //todo size? need resize?
            nested_column->get_data().push_back(vec[row_num]);
            DCHECK(null_map->size()==nested_column->size());
        }else{
            const auto& vec = assert_cast<const ColVecType&>(column).get_data();
            data.push_back(vec[row_num]);
        }
    }

    void add_new(const IColumn& column, size_t row_num) {
        if constexpr (Nullable){
            auto& to_arr = assert_cast<const ColumnArray&>(column);
            auto& to_nested_col = to_arr.get_data();
            auto col_null = reinterpret_cast<const ColumnNullable*>(&to_nested_col);
            const auto& vec=assert_cast<const ColVecType&>(col_null->get_nested_column()).get_data();
            auto row_nums=col_null->get_null_map_data().size();
            for(auto i=0;i<row_nums;++i){
                null_map->push_back(col_null->get_null_map_data()[i]);
                nested_column->get_data().push_back(vec[i]);
            }
        }else{
            const auto& vec = assert_cast<const ColVecType&>(column).get_data();
            data.push_back(vec[row_num]);
        }
    }

    void add_new_new(const IColumn& column, size_t row_num){
        if constexpr (Nullable){
            auto& to_arr = assert_cast<const ColumnArray&>(column);
            auto& to_nested_col = to_arr.get_data();
            auto col_null = reinterpret_cast<const ColumnNullable*>(&to_nested_col);
            const auto& vec=assert_cast<const ColVecType&>(col_null->get_nested_column()).get_data();
            auto start=to_arr.get_offsets()[row_num-1];
            auto end = start + to_arr.get_offsets()[row_num] - to_arr.get_offsets()[row_num - 1];
            for(auto i=start;i<end;++i){
                null_map->push_back(col_null->get_null_map_data()[i]);
                nested_column->get_data().push_back(vec[i]);
            }
        }
    }

    void merge(const SelfType& rhs) {
        if constexpr (Nullable){

        }else{
            if constexpr (HasLimit::value) {
                DCHECK(max_size == -1 || max_size == rhs.max_size);
                max_size = rhs.max_size;
                for (auto& rhs_elem : rhs.data) {
                    if (size() >= max_size) {
                        return;
                    }
                    data.push_back(rhs_elem);
                }
            } else {
                data.insert(rhs.data.begin(), rhs.data.end());
            }
        }
    }

    void write(BufferWritable& buf) const {
        if constexpr (Nullable){

        }else{
            write_var_uint(size(), buf);
            buf.write(data.raw_data(), size() * sizeof(ElementType));
            write_var_int(max_size, buf);
        }
    }

    void read(BufferReadable& buf) {
        if constexpr (Nullable){

        }else{
            UInt64 rows = 0;
            read_var_uint(rows, buf);
            data.resize(rows);
            buf.read(reinterpret_cast<char*>(data.data()), rows * sizeof(ElementType));
            read_var_int(max_size, buf);
        }
    }

    void reset() { data.clear(); }

    void insert_result_into(IColumn& to, bool shownull = false) const {
        if (shownull == false) {
            auto& vec = assert_cast<ColVecType&>(to).get_data();
            size_t old_size = vec.size();
            vec.resize(old_size + size());
            memcpy(vec.data() + old_size, data.data(), size() * sizeof(ElementType));
        } else {
            DCHECK(0);
            auto& to_arr = assert_cast<ColumnArray&>(to);
            auto& to_nested_col = to_arr.get_data();
            auto col_null = reinterpret_cast<ColumnNullable*>(&to_nested_col);
            auto& vec = assert_cast<ColVecType&>(col_null->get_nested_column()).get_data();
            size_t old_size = vec.size();
            size_t num_rows = data.size();
            vec.resize(old_size + num_rows);
            memcpy(vec.data() + old_size, data.data(), num_rows * sizeof(ElementType));
            DCHECK(to_nested_col.is_nullable());
            for (size_t i = 0; i < num_rows; ++i) {
                auto column = data[i];
                if (column) {
                    col_null->get_null_map_data().push_back(0);
                } else {
                    col_null->get_null_map_data().push_back(1);
                }
            }
            to_arr.get_offsets().push_back(to_nested_col.size());
        }
    }

    void insert_result_into_new(IColumn& to) const{

        if constexpr (Nullable){
            auto& to_arr = assert_cast<ColumnArray&>(to);
            auto& to_nested_col = to_arr.get_data();
            auto col_null = reinterpret_cast<ColumnNullable*>(&to_nested_col);
            auto& vec = assert_cast<ColVecType&>(col_null->get_nested_column()).get_data();
            size_t num_rows=null_map->size();
            auto& nested_column_data=nested_column->get_data();
            for (size_t i = 0; i < num_rows; ++i){
                col_null->get_null_map_data().push_back((*null_map)[i]);
                vec.push_back(nested_column_data[i]);
            }
            to_arr.get_offsets().push_back(to_nested_col.size());
        }
    }

};

template <typename HasLimit,bool Nullable>
struct AggregateFunctionCollectListData<StringRef, HasLimit,Nullable> {
    using ElementType = StringRef;
    using ColVecType = ColumnString;
    MutableColumnPtr data;
    Int64 max_size = -1;
    /**add by alex*/
    MutableColumnPtr column_data;
    ColVecType* nested_column;
    NullMap* null_map;
    /**end*/

    AggregateFunctionCollectListData() {
        if constexpr (Nullable)
        {
            column_data = ColumnNullable::create(ColVecType::create(),ColumnUInt8::create());
            null_map=&(assert_cast<ColumnNullable&>(*column_data).get_null_map_data());
            nested_column= assert_cast<ColVecType*>(assert_cast<ColumnNullable&>(*column_data).get_nested_column_ptr().get());
        } else{
            data = ColVecType::create();
        }
    }

    size_t size() const { return data->size(); }

    void add(const IColumn& column, size_t row_num) {
        if constexpr (Nullable){
            DCHECK(null_map->size()==nested_column->size());
            const auto& col= assert_cast<const ColumnNullable&>(column);
            const auto& vec=assert_cast<const ColVecType&>(col.get_nested_column());
            null_map->push_back(col.get_null_map_data()[row_num]);
            //todo size? need resize?
            nested_column->insert_from(vec,row_num);
            DCHECK(null_map->size()==nested_column->size());
        }else{
            data->insert_from(column, row_num);
        }
    }

    void add_new_new(const IColumn& column, size_t row_num){}

    //todo : bug exist
    void add_new(const IColumn& column, size_t row_num){
        if constexpr (Nullable){
            DCHECK(null_map->size()==nested_column->size());
            const auto& col= assert_cast<const ColumnNullable&>(column);
            const auto& vec=assert_cast<const ColVecType&>(col.get_nested_column());
            null_map->push_back(col.get_null_map_data()[row_num]);
            //todo size? need resize?
            nested_column->insert_from(vec,row_num);
            DCHECK(null_map->size()==nested_column->size());
        }
    };
    void merge(const AggregateFunctionCollectListData& rhs) {
        if constexpr (HasLimit::value) {
            DCHECK(max_size == -1 || max_size == rhs.max_size);
            max_size = rhs.max_size;

            data->insert_range_from(*rhs.data, 0,
                                    std::min(assert_cast<size_t>(max_size - size()), rhs.size()));
        } else {
            data->insert_range_from(*rhs.data, 0, rhs.size());
        }
    }

    void write(BufferWritable& buf) const {
        auto& col = assert_cast<ColVecType&>(*data);

        write_var_uint(col.size(), buf);
        buf.write(col.get_offsets().raw_data(), col.size() * sizeof(IColumn::Offset));

        write_var_uint(col.get_chars().size(), buf);
        buf.write(col.get_chars().raw_data(), col.get_chars().size());
        write_var_int(max_size, buf);
    }

    void read(BufferReadable& buf) {
        auto& col = assert_cast<ColVecType&>(*data);
        UInt64 offs_size = 0;
        read_var_uint(offs_size, buf);
        col.get_offsets().resize(offs_size);
        buf.read(reinterpret_cast<char*>(col.get_offsets().data()),
                 offs_size * sizeof(IColumn::Offset));

        UInt64 chars_size = 0;
        read_var_uint(chars_size, buf);
        col.get_chars().resize(chars_size);
        buf.read(reinterpret_cast<char*>(col.get_chars().data()), chars_size);
        read_var_int(max_size, buf);
    }

    void reset() { data->clear(); }

    void insert_result_into(IColumn& to, bool shownull = false) const {
        if (shownull == false) {
            auto& to_str = assert_cast<ColVecType&>(to);
            to_str.insert_range_from(*data, 0, size());
        } else {
            auto& to_arr = assert_cast<ColumnArray&>(to);
            auto& to_nested_col = to_arr.get_data();
            size_t num_rows = data->size();
            DCHECK(to_nested_col.is_nullable());
            auto col_null = reinterpret_cast<ColumnNullable*>(&to_nested_col);
            for (size_t i = 0; i < num_rows; ++i) {
                auto column = data->get_data_at(i);
                assert_cast<ColVecType&>(col_null->get_nested_column())
                        .insert_data(column.data, column.size);
                if (column.data == NULL || column.size == 0) {
                    col_null->get_null_map_data().push_back(1);
                } else {
                    col_null->get_null_map_data().push_back(0);
                }
            }
            to_arr.get_offsets().push_back(to_nested_col.size());
        }
    }

    void insert_result_into_new(IColumn& to) const{}
};

//ShowNull is just used to support array_agg because array_agg needs to display NULL
//todo: Supports order by sorting for array_agg
template <typename Data, typename HasLimit, typename ShowNull>
class AggregateFunctionCollect
        : public IAggregateFunctionDataHelper<Data,
                                              AggregateFunctionCollect<Data, HasLimit, ShowNull>> {
    using GenericType = AggregateFunctionCollectSetData<StringRef, HasLimit>;

    static constexpr bool ENABLE_ARENA = std::is_same_v<Data, GenericType>;

public:
    using BaseHelper = IAggregateFunctionHelper<AggregateFunctionCollect<Data, HasLimit, ShowNull>>;

    AggregateFunctionCollect(const DataTypes& argument_types,
                             UInt64 max_size_ = std::numeric_limits<UInt64>::max())
            : IAggregateFunctionDataHelper<Data,
                                           AggregateFunctionCollect<Data, HasLimit, ShowNull>>(
                      {argument_types}),
              return_type(argument_types[0]) {}

    std::string get_name() const override {
        if constexpr (ShowNull::value) {
            return "array_agg";
        } else if constexpr (std::is_same_v<AggregateFunctionCollectListData<
                                                    typename Data::ElementType, HasLimit,false>,
                                            Data>) {
            return "collect_list";
        } else {
            return "collect_set";
        }
    }

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeArray>(make_nullable(return_type));
    }

    bool allocates_memory_in_arena() const override { return ENABLE_ARENA; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena* arena) const override {
        auto& data = this->data(place);
        if constexpr (HasLimit::value) {
            if (data.max_size == -1) {
                data.max_size =
                        (UInt64)assert_cast<const ColumnInt32*>(columns[1])->get_element(row_num);
            }
            if (data.size() >= data.max_size) {
                return;
            }
        }
        if constexpr (ENABLE_ARENA) {
            data.add(*columns[0], row_num, arena);
        } else {
            if (ShowNull::value && columns[0]->is_nullable()) {
                //auto& nullable_col = assert_cast<const ColumnNullable&>(*columns[0]);
                data.add(*columns[0], row_num);
            } else {
                data.add(*columns[0], row_num);
            }
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena* arena) const override {
        auto& data = this->data(place);
        auto& rhs_data = this->data(rhs);
        if constexpr (ENABLE_ARENA) {
            data.merge(rhs_data, arena);
        } else {
            data.merge(rhs_data);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& to_arr = assert_cast<ColumnArray&>(to);
        auto& to_nested_col = to_arr.get_data();
        if constexpr (ShowNull::value) {
            DCHECK(to_nested_col.is_nullable());
            this->data(place).insert_result_into_new(to);
        } else {
            if (to_nested_col.is_nullable()) {
                auto col_null = reinterpret_cast<ColumnNullable*>(&to_nested_col);
                this->data(place).insert_result_into(col_null->get_nested_column());
                col_null->get_null_map_data().resize_fill(col_null->get_nested_column().size(), 0);
            } else {
                this->data(place).insert_result_into(to_nested_col);
            }
            to_arr.get_offsets().push_back(to_nested_col.size());
        }
    }

    void serialize_without_key_to_column(ConstAggregateDataPtr __restrict place,
                                         IColumn& to)const override {
        if constexpr (ShowNull::value){
            this->data(place).insert_result_into_new(to);
        }else{
            return BaseHelper::serialize_without_key_to_column(place,to);
        }
    }

    void deserialize_and_merge_from_column(AggregateDataPtr __restrict place, const IColumn& column,
                                           Arena* arena) const override {
        if constexpr (ShowNull::value){
            const size_t num_rows = column.size();
            for (size_t i = 0; i != num_rows; ++i){
                this->data(place).add_new(column,i);
            }
        }else{
            return BaseHelper::deserialize_and_merge_from_column(place, column, arena);
        }
    }

    void deserialize_and_merge_vec(const AggregateDataPtr* places, size_t offset,
                                   AggregateDataPtr rhs, const ColumnString* column, Arena* arena,
                                   const size_t num_rows) const override{
        if constexpr (ShowNull::value){
          auto& col = assert_cast<const ColumnArray&>(*assert_cast<const IColumn*>(column));
          auto sizedd=col.size();
            DCHECK(sizedd==num_rows);
            for (size_t i = 0; i != num_rows; ++i){
                this->data(places[i]).add_new_new(col,i);
            }
        }
    }

    void serialize_to_column(const std::vector<AggregateDataPtr>& places, size_t offset,
                             MutableColumnPtr& dst, const size_t num_rows) const override{
        if constexpr (ShowNull::value){
            for (size_t i = 0; i != num_rows; ++i) {
                Data& data_ = this->data(places[i] + offset);
                data_.insert_result_into_new(*dst);
            }
        }
    }


    [[nodiscard]] MutableColumnPtr create_serialize_column() const override{
       // using KeyColumnType =
         //       std::conditional_t<std::is_same_v<String, typename Data::ElementType>, ColumnString, ColumnVectorOrDecimal<typename Data::ElementType>>;
        if constexpr(ShowNull::value){
            if constexpr(IsDecimalNumber<typename Data::ElementType>){
                return ColumnString::create();
            } else {
                return  get_return_type()->create_column();
               // return ColumnNullable::create(KeyColumnType::create(),ColumnUInt8::create());
            }
                //return ColumnNullable::create(KeyColumnType::create(),ColumnUInt8::create());

        }
        return ColumnString::create();
    }


    [[nodiscard]] DataTypePtr get_serialized_type() const override {
        if constexpr(ShowNull::value){
            return std::make_shared<DataTypeArray>(make_nullable(return_type));
        } else {
            return IAggregateFunction::get_serialized_type();
        }
    }

private:
    DataTypePtr return_type;
};

} // namespace doris::vectorized