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

#include "olap/base_tablet.h"

#include <fmt/format.h>

#include "olap/calc_delete_bitmap_executor.h"
#include "olap/delete_bitmap_calculator.h"
#include "olap/memtable.h"
#include "olap/primary_key_index.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/tablet_fwd.h"
#include "service/point_query_executor.h"
#include "util/bvar_helper.h"
#include "util/doris_metrics.h"
#include "vec/common/schema_util.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/jsonb/serialize.h"

namespace doris {
using namespace ErrorCode;

namespace {

bvar::LatencyRecorder g_tablet_commit_phase_update_delete_bitmap_latency(
        "doris_pk", "commit_phase_update_delete_bitmap");
bvar::LatencyRecorder g_tablet_lookup_rowkey_latency("doris_pk", "tablet_lookup_rowkey");
bvar::Adder<uint64_t> g_tablet_pk_not_found("doris_pk", "lookup_not_found");
bvar::PerSecond<bvar::Adder<uint64_t>> g_tablet_pk_not_found_per_second(
        "doris_pk", "lookup_not_found_per_second", &g_tablet_pk_not_found, 60);

// read columns by read plan
// read_index: ori_pos-> block_idx
Status read_columns_by_plan(TabletSchemaSPtr tablet_schema,
                            const std::vector<uint32_t> cids_to_read,
                            const PartialUpdateReadPlan& read_plan,
                            const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
                            vectorized::Block& block, std::map<uint32_t, uint32_t>* read_index) {
    bool has_row_column = tablet_schema->store_row_column();
    auto mutable_columns = block.mutate_columns();
    size_t read_idx = 0;
    for (auto rs_it : read_plan) {
        for (auto seg_it : rs_it.second) {
            auto rowset_iter = rsid_to_rowset.find(rs_it.first);
            CHECK(rowset_iter != rsid_to_rowset.end());
            std::vector<uint32_t> rids;
            for (auto id_and_pos : seg_it.second) {
                rids.emplace_back(id_and_pos.rid);
                (*read_index)[id_and_pos.pos] = read_idx++;
            }
            if (has_row_column) {
                auto st = BaseTablet::fetch_value_through_row_column(rowset_iter->second,
                                                                     *tablet_schema, seg_it.first,
                                                                     rids, cids_to_read, block);
                if (!st.ok()) {
                    LOG(WARNING) << "failed to fetch value through row column";
                    return st;
                }
                continue;
            }
            for (size_t cid = 0; cid < mutable_columns.size(); ++cid) {
                TabletColumn tablet_column = tablet_schema->column(cids_to_read[cid]);
                auto st = BaseTablet::fetch_value_by_rowids(rowset_iter->second, seg_it.first, rids,
                                                            tablet_column, mutable_columns[cid]);
                // set read value to output block
                if (!st.ok()) {
                    LOG(WARNING) << "failed to fetch value";
                    return st;
                }
            }
        }
    }
    block.set_columns(std::move(mutable_columns));
    return Status::OK();
}

Status _get_segment_column_iterator(const BetaRowsetSharedPtr& rowset, uint32_t segid,
                                    const TabletColumn& target_column,
                                    SegmentCacheHandle* segment_cache_handle,
                                    std::unique_ptr<segment_v2::ColumnIterator>* column_iterator,
                                    OlapReaderStatistics* stats) {
    RETURN_IF_ERROR(SegmentLoader::instance()->load_segments(rowset, segment_cache_handle, true));
    // find segment
    auto it = std::find_if(
            segment_cache_handle->get_segments().begin(),
            segment_cache_handle->get_segments().end(),
            [&segid](const segment_v2::SegmentSharedPtr& seg) { return seg->id() == segid; });
    if (it == segment_cache_handle->get_segments().end()) {
        return Status::NotFound(fmt::format("rowset {} 's segemnt not found, seg_id {}",
                                            rowset->rowset_id().to_string(), segid));
    }
    segment_v2::SegmentSharedPtr segment = *it;
    RETURN_IF_ERROR(segment->new_column_iterator(target_column, column_iterator, nullptr));
    segment_v2::ColumnIteratorOptions opt {
            .use_page_cache = !config::disable_storage_page_cache,
            .file_reader = segment->file_reader().get(),
            .stats = stats,
            .io_ctx = io::IOContext {.reader_type = ReaderType::READER_QUERY},
    };
    RETURN_IF_ERROR((*column_iterator)->init(opt));
    return Status::OK();
}

} // namespace

extern MetricPrototype METRIC_query_scan_bytes;
extern MetricPrototype METRIC_query_scan_rows;
extern MetricPrototype METRIC_query_scan_count;
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(flush_bytes, MetricUnit::BYTES);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(flush_finish_count, MetricUnit::OPERATIONS);

BaseTablet::BaseTablet(TabletMetaSharedPtr tablet_meta) : _tablet_meta(std::move(tablet_meta)) {
    _metric_entity = DorisMetrics::instance()->metric_registry()->register_entity(
            fmt::format("Tablet.{}", tablet_id()), {{"tablet_id", std::to_string(tablet_id())}},
            MetricEntityType::kTablet);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, query_scan_bytes);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, query_scan_rows);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, query_scan_count);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, flush_bytes);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, flush_finish_count);

    // construct _timestamped_versioned_tracker from rs and stale rs meta
    _timestamped_version_tracker.construct_versioned_tracker(_tablet_meta->all_rs_metas(),
                                                             _tablet_meta->all_stale_rs_metas());

    // if !_tablet_meta->all_rs_metas()[0]->tablet_schema(),
    // that mean the tablet_meta is still no upgrade to doris 1.2 versions.
    // Before doris 1.2 version, rowset metas don't have tablet schema.
    // And when upgrade to doris 1.2 version,
    // all rowset metas will be set the tablet schmea from tablet meta.
    if (_tablet_meta->all_rs_metas().empty() || !_tablet_meta->all_rs_metas()[0]->tablet_schema()) {
        _max_version_schema = _tablet_meta->tablet_schema();
    } else {
        _max_version_schema =
                tablet_schema_with_merged_max_schema_version(_tablet_meta->all_rs_metas());
    }
    DCHECK(_max_version_schema);
}

BaseTablet::~BaseTablet() {
    DorisMetrics::instance()->metric_registry()->deregister_entity(_metric_entity);
}

TabletSchemaSPtr BaseTablet::tablet_schema_with_merged_max_schema_version(
        const std::vector<RowsetMetaSharedPtr>& rowset_metas) {
    RowsetMetaSharedPtr max_schema_version_rs = *std::max_element(
            rowset_metas.begin(), rowset_metas.end(),
            [](const RowsetMetaSharedPtr& a, const RowsetMetaSharedPtr& b) {
                return !a->tablet_schema()
                               ? true
                               : (!b->tablet_schema()
                                          ? false
                                          : a->tablet_schema()->schema_version() <
                                                    b->tablet_schema()->schema_version());
            });
    TabletSchemaSPtr target_schema = max_schema_version_rs->tablet_schema();
    if (target_schema->num_variant_columns() > 0) {
        // For variant columns tablet schema need to be the merged wide tablet schema
        std::vector<TabletSchemaSPtr> schemas;
        std::transform(rowset_metas.begin(), rowset_metas.end(), std::back_inserter(schemas),
                       [](const RowsetMetaSharedPtr& rs_meta) { return rs_meta->tablet_schema(); });
        static_cast<void>(
                vectorized::schema_util::get_least_common_schema(schemas, nullptr, target_schema));
        VLOG_DEBUG << "dump schema: " << target_schema->dump_structure();
    }
    return target_schema;
}

Status BaseTablet::set_tablet_state(TabletState state) {
    if (_tablet_meta->tablet_state() == TABLET_SHUTDOWN && state != TABLET_SHUTDOWN) {
        return Status::Error<META_INVALID_ARGUMENT>(
                "could not change tablet state from shutdown to {}", state);
    }
    _tablet_meta->set_tablet_state(state);
    return Status::OK();
}

void BaseTablet::update_max_version_schema(const TabletSchemaSPtr& tablet_schema) {
    std::lock_guard wrlock(_meta_lock);
    // Double Check for concurrent update
    if (!_max_version_schema ||
        tablet_schema->schema_version() > _max_version_schema->schema_version()) {
        _max_version_schema = tablet_schema;
    }
}

Status BaseTablet::update_by_least_common_schema(const TabletSchemaSPtr& update_schema) {
    std::lock_guard wrlock(_meta_lock);
    CHECK(_max_version_schema->schema_version() >= update_schema->schema_version());
    TabletSchemaSPtr final_schema;
    bool check_column_size = true;
    RETURN_IF_ERROR(vectorized::schema_util::get_least_common_schema(
            {_max_version_schema, update_schema}, _max_version_schema, final_schema,
            check_column_size));
    _max_version_schema = final_schema;
    VLOG_DEBUG << "dump updated tablet schema: " << final_schema->dump_structure();
    return Status::OK();
}

Status BaseTablet::capture_rs_readers_unlocked(const Versions& version_path,
                                               std::vector<RowSetSplits>* rs_splits) const {
    DCHECK(rs_splits != nullptr && rs_splits->empty());
    for (auto version : version_path) {
        auto it = _rs_version_map.find(version);
        if (it == _rs_version_map.end()) {
            VLOG_NOTICE << "fail to find Rowset in rs_version for version. tablet=" << tablet_id()
                        << ", version='" << version.first << "-" << version.second;

            it = _stale_rs_version_map.find(version);
            if (it == _stale_rs_version_map.end()) {
                return Status::Error<CAPTURE_ROWSET_READER_ERROR>(
                        "fail to find Rowset in stale_rs_version for version. tablet={}, "
                        "version={}-{}",
                        tablet_id(), version.first, version.second);
            }
        }
        RowsetReaderSharedPtr rs_reader;
        auto res = it->second->create_reader(&rs_reader);
        if (!res.ok()) {
            return Status::Error<CAPTURE_ROWSET_READER_ERROR>(
                    "failed to create reader for rowset:{}", it->second->rowset_id().to_string());
        }
        rs_splits->emplace_back(std::move(rs_reader));
    }
    return Status::OK();
}

// snapshot manager may call this api to check if version exists, so that
// the version maybe not exist
RowsetSharedPtr BaseTablet::get_rowset_by_version(const Version& version,
                                                  bool find_in_stale) const {
    auto iter = _rs_version_map.find(version);
    if (iter == _rs_version_map.end()) {
        if (find_in_stale) {
            return get_stale_rowset_by_version(version);
        }
        return nullptr;
    }
    return iter->second;
}

RowsetSharedPtr BaseTablet::get_stale_rowset_by_version(const Version& version) const {
    auto iter = _stale_rs_version_map.find(version);
    if (iter == _stale_rs_version_map.end()) {
        VLOG_NOTICE << "no rowset for version:" << version << ", tablet: " << tablet_id();
        return nullptr;
    }
    return iter->second;
}

// Already under _meta_lock
RowsetSharedPtr BaseTablet::get_rowset_with_max_version() const {
    Version max_version = _tablet_meta->max_version();
    if (max_version.first == -1) {
        return nullptr;
    }

    auto iter = _rs_version_map.find(max_version);
    if (iter == _rs_version_map.end()) {
        DCHECK(false) << "invalid version:" << max_version;
        return nullptr;
    }
    return iter->second;
}

Status BaseTablet::get_all_rs_id(int64_t max_version, RowsetIdUnorderedSet* rowset_ids) const {
    std::shared_lock rlock(_meta_lock);
    return get_all_rs_id_unlocked(max_version, rowset_ids);
}

Status BaseTablet::get_all_rs_id_unlocked(int64_t max_version,
                                          RowsetIdUnorderedSet* rowset_ids) const {
    //  Ensure that the obtained versions of rowsets are continuous
    Version spec_version(0, max_version);
    Versions version_path;
    auto st = _timestamped_version_tracker.capture_consistent_versions(spec_version, &version_path);
    if (!st.ok()) [[unlikely]] {
        return st;
    }

    for (auto& ver : version_path) {
        if (ver.second == 1) {
            // [0-1] rowset is empty for each tablet, skip it
            continue;
        }
        auto it = _rs_version_map.find(ver);
        if (it == _rs_version_map.end()) {
            return Status::Error<CAPTURE_ROWSET_ERROR, false>(
                    "fail to find Rowset for version. tablet={}, version={}", tablet_id(),
                    ver.to_string());
        }
        rowset_ids->emplace(it->second->rowset_id());
    }
    return Status::OK();
}

Versions BaseTablet::get_missed_versions(int64_t spec_version) const {
    DCHECK(spec_version > 0) << "invalid spec_version: " << spec_version;

    Versions existing_versions;
    {
        std::shared_lock rdlock(_meta_lock);
        for (const auto& rs : _tablet_meta->all_rs_metas()) {
            existing_versions.emplace_back(rs->version());
        }
    }
    return calc_missed_versions(spec_version, existing_versions);
}

Versions BaseTablet::get_missed_versions_unlocked(int64_t spec_version) const {
    DCHECK(spec_version > 0) << "invalid spec_version: " << spec_version;

    Versions existing_versions;
    for (const auto& rs : _tablet_meta->all_rs_metas()) {
        existing_versions.emplace_back(rs->version());
    }
    return calc_missed_versions(spec_version, existing_versions);
}

Versions BaseTablet::calc_missed_versions(int64_t spec_version, Versions existing_versions) {
    DCHECK(spec_version > 0) << "invalid spec_version: " << spec_version;

    // sort the existing versions in ascending order
    std::sort(existing_versions.begin(), existing_versions.end(),
              [](const Version& a, const Version& b) {
                  // simple because 2 versions are certainly not overlapping
                  return a.first < b.first;
              });

    // From the first version(=0), find the missing version until spec_version
    int64_t last_version = -1;
    Versions missed_versions;
    for (const Version& version : existing_versions) {
        if (version.first > last_version + 1) {
            // there is a hole between versions
            missed_versions.emplace_back(last_version + 1, std::min(version.first, spec_version));
        }
        last_version = version.second;
        if (last_version >= spec_version) {
            break;
        }
    }
    if (last_version < spec_version) {
        // there is a hole between the last version and the specificed version.
        missed_versions.emplace_back(last_version + 1, spec_version);
    }
    return missed_versions;
}

void BaseTablet::_print_missed_versions(const Versions& missed_versions) const {
    std::stringstream ss;
    ss << tablet_id() << " has " << missed_versions.size() << " missed version:";
    // print at most 10 version
    for (int i = 0; i < 10 && i < missed_versions.size(); ++i) {
        ss << missed_versions[i] << ",";
    }
    LOG(WARNING) << ss.str();
}

bool BaseTablet::_reconstruct_version_tracker_if_necessary() {
    double orphan_vertex_ratio = _timestamped_version_tracker.get_orphan_vertex_ratio();
    if (orphan_vertex_ratio >= config::tablet_version_graph_orphan_vertex_ratio) {
        _timestamped_version_tracker.construct_versioned_tracker(
                _tablet_meta->all_rs_metas(), _tablet_meta->all_stale_rs_metas());
        return true;
    }
    return false;
}

// should use this method to get a copy of current tablet meta
// there are some rowset meta in local meta store and in in-memory tablet meta
// but not in tablet meta in local meta store
void BaseTablet::generate_tablet_meta_copy(TabletMeta& new_tablet_meta) const {
    TabletMetaPB tablet_meta_pb;
    {
        std::shared_lock rdlock(_meta_lock);
        _tablet_meta->to_meta_pb(&tablet_meta_pb);
    }
    generate_tablet_meta_copy_unlocked(new_tablet_meta);
}

// this is a unlocked version of generate_tablet_meta_copy()
// some method already hold the _meta_lock before calling this,
// such as EngineCloneTask::_finish_clone -> tablet->revise_tablet_meta
void BaseTablet::generate_tablet_meta_copy_unlocked(TabletMeta& new_tablet_meta) const {
    TabletMetaPB tablet_meta_pb;
    _tablet_meta->to_meta_pb(&tablet_meta_pb);
    new_tablet_meta.init_from_pb(tablet_meta_pb);
}

Status BaseTablet::calc_delete_bitmap_between_segments(
        RowsetSharedPtr rowset, const std::vector<segment_v2::SegmentSharedPtr>& segments,
        DeleteBitmapPtr delete_bitmap) {
    size_t const num_segments = segments.size();
    if (num_segments < 2) {
        return Status::OK();
    }

    OlapStopWatch watch;
    auto const rowset_id = rowset->rowset_id();
    size_t seq_col_length = 0;
    if (_tablet_meta->tablet_schema()->has_sequence_col()) {
        auto seq_col_idx = _tablet_meta->tablet_schema()->sequence_col_idx();
        seq_col_length = _tablet_meta->tablet_schema()->column(seq_col_idx).length() + 1;
    }
    size_t rowid_length = 0;
    if (!_tablet_meta->tablet_schema()->cluster_key_idxes().empty()) {
        rowid_length = PrimaryKeyIndexReader::ROW_ID_LENGTH;
    }

    MergeIndexDeleteBitmapCalculator calculator;
    RETURN_IF_ERROR(calculator.init(rowset_id, segments, seq_col_length, rowid_length));

    RETURN_IF_ERROR(calculator.calculate_all(delete_bitmap));

    LOG(INFO) << fmt::format(
            "construct delete bitmap between segments, "
            "tablet: {}, rowset: {}, number of segments: {}, bitmap size: {}, cost {} (us)",
            tablet_id(), rowset_id.to_string(), num_segments, delete_bitmap->delete_bitmap.size(),
            watch.get_elapse_time_us());
    return Status::OK();
}

std::vector<RowsetSharedPtr> BaseTablet::get_rowset_by_ids(
        const RowsetIdUnorderedSet* specified_rowset_ids) {
    std::vector<RowsetSharedPtr> rowsets;
    for (auto& rs : _rs_version_map) {
        if (!specified_rowset_ids ||
            specified_rowset_ids->find(rs.second->rowset_id()) != specified_rowset_ids->end()) {
            rowsets.push_back(rs.second);
        }
    }
    std::sort(rowsets.begin(), rowsets.end(), [](RowsetSharedPtr& lhs, RowsetSharedPtr& rhs) {
        return lhs->end_version() > rhs->end_version();
    });
    return rowsets;
}

Status BaseTablet::lookup_row_data(const Slice& encoded_key, const RowLocation& row_location,
                                   RowsetSharedPtr input_rowset, const TupleDescriptor* desc,
                                   OlapReaderStatistics& stats, std::string& values,
                                   bool write_to_cache) {
    MonotonicStopWatch watch;
    size_t row_size = 1;
    watch.start();
    Defer _defer([&]() {
        LOG_EVERY_N(INFO, 500) << "get a single_row, cost(us):" << watch.elapsed_time() / 1000
                               << ", row_size:" << row_size;
    });

    BetaRowsetSharedPtr rowset = std::static_pointer_cast<BetaRowset>(input_rowset);
    CHECK(rowset);
    const TabletSchemaSPtr tablet_schema = rowset->tablet_schema();
    CHECK(tablet_schema->store_row_column());
    SegmentCacheHandle segment_cache_handle;
    std::unique_ptr<segment_v2::ColumnIterator> column_iterator;
    RETURN_IF_ERROR(_get_segment_column_iterator(rowset, row_location.segment_id,
                                                 tablet_schema->column(BeConsts::ROW_STORE_COL),
                                                 &segment_cache_handle, &column_iterator, &stats));
    // get and parse tuple row
    vectorized::MutableColumnPtr column_ptr = vectorized::ColumnString::create();
    std::vector<segment_v2::rowid_t> rowids {static_cast<segment_v2::rowid_t>(row_location.row_id)};
    RETURN_IF_ERROR(column_iterator->read_by_rowids(rowids.data(), 1, column_ptr));
    assert(column_ptr->size() == 1);
    auto* string_column = static_cast<vectorized::ColumnString*>(column_ptr.get());
    StringRef value = string_column->get_data_at(0);
    values = value.to_string();
    if (write_to_cache) {
        StringRef value = string_column->get_data_at(0);
        RowCache::instance()->insert({tablet_id(), encoded_key}, Slice {value.data, value.size});
    }
    return Status::OK();
}

Status BaseTablet::lookup_row_key(const Slice& encoded_key, bool with_seq_col,
                                  const std::vector<RowsetSharedPtr>& specified_rowsets,
                                  RowLocation* row_location, uint32_t version,
                                  std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches,
                                  RowsetSharedPtr* rowset, bool with_rowid) {
    SCOPED_BVAR_LATENCY(g_tablet_lookup_rowkey_latency);
    size_t seq_col_length = 0;
    if (_tablet_meta->tablet_schema()->has_sequence_col() && with_seq_col) {
        seq_col_length = _tablet_meta->tablet_schema()
                                 ->column(_tablet_meta->tablet_schema()->sequence_col_idx())
                                 .length() +
                         1;
    }
    size_t rowid_length = 0;
    if (with_rowid && !_tablet_meta->tablet_schema()->cluster_key_idxes().empty()) {
        rowid_length = PrimaryKeyIndexReader::ROW_ID_LENGTH;
    }
    Slice key_without_seq =
            Slice(encoded_key.get_data(), encoded_key.get_size() - seq_col_length - rowid_length);
    RowLocation loc;

    for (size_t i = 0; i < specified_rowsets.size(); i++) {
        auto& rs = specified_rowsets[i];
        auto& segments_key_bounds = rs->rowset_meta()->get_segments_key_bounds();
        int num_segments = rs->num_segments();
        DCHECK_EQ(segments_key_bounds.size(), num_segments);
        std::vector<uint32_t> picked_segments;
        for (int i = num_segments - 1; i >= 0; i--) {
            // If mow table has cluster keys, the key bounds is short keys, not primary keys
            // use PrimaryKeyIndexMetaPB in primary key index?
            if (_tablet_meta->tablet_schema()->cluster_key_idxes().empty()) {
                if (key_without_seq.compare(segments_key_bounds[i].max_key()) > 0 ||
                    key_without_seq.compare(segments_key_bounds[i].min_key()) < 0) {
                    continue;
                }
            }
            picked_segments.emplace_back(i);
        }
        if (picked_segments.empty()) {
            continue;
        }

        if (UNLIKELY(segment_caches[i] == nullptr)) {
            segment_caches[i] = std::make_unique<SegmentCacheHandle>();
            RETURN_IF_ERROR(SegmentLoader::instance()->load_segments(
                    std::static_pointer_cast<BetaRowset>(rs), segment_caches[i].get(), true));
        }
        auto& segments = segment_caches[i]->get_segments();
        DCHECK_EQ(segments.size(), num_segments);

        for (auto id : picked_segments) {
            Status s = segments[id]->lookup_row_key(encoded_key, with_seq_col, with_rowid, &loc);
            if (s.is<KEY_NOT_FOUND>()) {
                continue;
            }
            if (!s.ok() && !s.is<KEY_ALREADY_EXISTS>()) {
                return s;
            }
            if (s.ok() && _tablet_meta->delete_bitmap().contains_agg_without_cache(
                                  {loc.rowset_id, loc.segment_id, version}, loc.row_id)) {
                // if has sequence col, we continue to compare the sequence_id of
                // all rowsets, util we find an existing key.
                if (_tablet_meta->tablet_schema()->has_sequence_col()) {
                    continue;
                }
                // The key is deleted, we don't need to search for it any more.
                break;
            }
            // `st` is either OK or KEY_ALREADY_EXISTS now.
            // for partial update, even if the key is already exists, we still need to
            // read it's original values to keep all columns align.
            *row_location = loc;
            if (rowset) {
                // return it's rowset
                *rowset = rs;
            }
            // find it and return
            return s;
        }
    }
    g_tablet_pk_not_found << 1;
    return Status::Error<ErrorCode::KEY_NOT_FOUND>("can't find key in all rowsets");
}

void BaseTablet::prepare_to_read(const RowLocation& row_location, size_t pos,
                                 PartialUpdateReadPlan* read_plan) {
    auto rs_it = read_plan->find(row_location.rowset_id);
    if (rs_it == read_plan->end()) {
        std::map<uint32_t, std::vector<RidAndPos>> segid_to_rid;
        std::vector<RidAndPos> rid_pos;
        rid_pos.emplace_back(RidAndPos {row_location.row_id, pos});
        segid_to_rid.emplace(row_location.segment_id, rid_pos);
        read_plan->emplace(row_location.rowset_id, segid_to_rid);
        return;
    }
    auto seg_it = rs_it->second.find(row_location.segment_id);
    if (seg_it == rs_it->second.end()) {
        std::vector<RidAndPos> rid_pos;
        rid_pos.emplace_back(RidAndPos {row_location.row_id, pos});
        rs_it->second.emplace(row_location.segment_id, rid_pos);
        return;
    }
    seg_it->second.emplace_back(RidAndPos {row_location.row_id, pos});
}

// if user pass a token, then all calculation works will submit to a threadpool,
// user can get all delete bitmaps from that token.
// if `token` is nullptr, the calculation will run in local, and user can get the result
// delete bitmap from `delete_bitmap` directly.
Status BaseTablet::calc_delete_bitmap(const BaseTabletSPtr& tablet, RowsetSharedPtr rowset,
                                      const std::vector<segment_v2::SegmentSharedPtr>& segments,
                                      const std::vector<RowsetSharedPtr>& specified_rowsets,
                                      DeleteBitmapPtr delete_bitmap, int64_t end_version,
                                      CalcDeleteBitmapToken* token, RowsetWriter* rowset_writer) {
    auto rowset_id = rowset->rowset_id();
    if (specified_rowsets.empty() || segments.empty()) {
        LOG(INFO) << "skip to construct delete bitmap tablet: " << tablet->tablet_id()
                  << " rowset: " << rowset_id;
        return Status::OK();
    }

    OlapStopWatch watch;
    for (const auto& segment : segments) {
        const auto& seg = segment;
        if (token != nullptr) {
            RETURN_IF_ERROR(token->submit(tablet, rowset, seg, specified_rowsets, end_version,
                                          delete_bitmap, rowset_writer));
        } else {
            RETURN_IF_ERROR(tablet->calc_segment_delete_bitmap(
                    rowset, segment, specified_rowsets, delete_bitmap, end_version, rowset_writer));
        }
    }

    return Status::OK();
}

Status BaseTablet::calc_segment_delete_bitmap(RowsetSharedPtr rowset,
                                              const segment_v2::SegmentSharedPtr& seg,
                                              const std::vector<RowsetSharedPtr>& specified_rowsets,
                                              DeleteBitmapPtr delete_bitmap, int64_t end_version,
                                              RowsetWriter* rowset_writer) {
    OlapStopWatch watch;
    auto rowset_id = rowset->rowset_id();
    Version dummy_version(end_version + 1, end_version + 1);
    auto rowset_schema = rowset->tablet_schema();
    bool is_partial_update = rowset_writer && rowset_writer->is_partial_update();
    bool have_input_seq_column = false;
    if (is_partial_update && rowset_schema->has_sequence_col()) {
        std::vector<uint32_t> including_cids =
                rowset_writer->get_partial_update_info()->update_cids;
        have_input_seq_column =
                rowset_schema->has_sequence_col() &&
                (std::find(including_cids.cbegin(), including_cids.cend(),
                           rowset_schema->sequence_col_idx()) != including_cids.cend());
    }
    // use for partial update
    PartialUpdateReadPlan read_plan_ori;
    PartialUpdateReadPlan read_plan_update;

    std::map<RowsetId, RowsetSharedPtr> rsid_to_rowset;
    rsid_to_rowset[rowset_id] = rowset;
    vectorized::Block block = rowset_schema->create_block();
    vectorized::Block ordered_block = block.clone_empty();
    uint32_t pos = 0;

    RETURN_IF_ERROR(seg->load_pk_index_and_bf()); // We need index blocks to iterate
    const auto* pk_idx = seg->get_primary_key_index();
    int total = pk_idx->num_rows();
    uint32_t row_id = 0;
    int32_t remaining = total;
    bool exact_match = false;
    std::string last_key;
    int batch_size = 1024;
    // The data for each segment may be lookup multiple times. Creating a SegmentCacheHandle
    // will update the lru cache, and there will be obvious lock competition in multithreading
    // scenarios, so using a segment_caches to cache SegmentCacheHandle.
    std::vector<std::unique_ptr<SegmentCacheHandle>> segment_caches(specified_rowsets.size());
    while (remaining > 0) {
        std::unique_ptr<segment_v2::IndexedColumnIterator> iter;
        RETURN_IF_ERROR(pk_idx->new_iterator(&iter));

        size_t num_to_read = std::min(batch_size, remaining);
        auto index_type = vectorized::DataTypeFactory::instance().create_data_type(
                pk_idx->type_info()->type(), 1, 0);
        auto index_column = index_type->create_column();
        Slice last_key_slice(last_key);
        RETURN_IF_ERROR(iter->seek_at_or_after(&last_key_slice, &exact_match));
        auto current_ordinal = iter->get_current_ordinal();
        DCHECK(total == remaining + current_ordinal)
                << "total: " << total << ", remaining: " << remaining
                << ", current_ordinal: " << current_ordinal;

        size_t num_read = num_to_read;
        RETURN_IF_ERROR(iter->next_batch(&num_read, index_column));
        DCHECK(num_to_read == num_read)
                << "num_to_read: " << num_to_read << ", num_read: " << num_read;
        last_key = index_column->get_data_at(num_read - 1).to_string();

        // exclude last_key, last_key will be read in next batch.
        if (num_read == batch_size && num_read != remaining) {
            num_read -= 1;
        }
        for (size_t i = 0; i < num_read; i++, row_id++) {
            Slice key = Slice(index_column->get_data_at(i).data, index_column->get_data_at(i).size);
            RowLocation loc;
            // calculate row id
            if (!_tablet_meta->tablet_schema()->cluster_key_idxes().empty()) {
                size_t seq_col_length = 0;
                if (_tablet_meta->tablet_schema()->has_sequence_col()) {
                    seq_col_length =
                            _tablet_meta->tablet_schema()
                                    ->column(_tablet_meta->tablet_schema()->sequence_col_idx())
                                    .length() +
                            1;
                }
                size_t rowid_length = PrimaryKeyIndexReader::ROW_ID_LENGTH;
                Slice key_without_seq =
                        Slice(key.get_data(), key.get_size() - seq_col_length - rowid_length);
                Slice rowid_slice =
                        Slice(key.get_data() + key_without_seq.get_size() + seq_col_length + 1,
                              rowid_length - 1);
                const auto* type_info =
                        get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>();
                const auto* rowid_coder = get_key_coder(type_info->type());
                RETURN_IF_ERROR(rowid_coder->decode_ascending(&rowid_slice, rowid_length,
                                                              (uint8_t*)&row_id));
            }
            // same row in segments should be filtered
            if (delete_bitmap->contains({rowset_id, seg->id(), DeleteBitmap::TEMP_VERSION_COMMON},
                                        row_id)) {
                continue;
            }

            RowsetSharedPtr rowset_find;
            auto st = lookup_row_key(key, true, specified_rowsets, &loc, dummy_version.first - 1,
                                     segment_caches, &rowset_find);
            bool expected_st = st.ok() || st.is<KEY_NOT_FOUND>() || st.is<KEY_ALREADY_EXISTS>();
            // It's a defensive DCHECK, we need to exclude some common errors to avoid core-dump
            // while stress test
            DCHECK(expected_st || st.is<MEM_LIMIT_EXCEEDED>())
                    << "unexpected error status while lookup_row_key:" << st;
            if (!expected_st) {
                return st;
            }
            if (st.is<KEY_NOT_FOUND>()) {
                continue;
            }

            if (st.is<KEY_ALREADY_EXISTS>() && (!is_partial_update || have_input_seq_column)) {
                // `st.is<KEY_ALREADY_EXISTS>()` means that there exists a row with the same key and larger value
                // in seqeunce column.
                // - If the current load is not a partial update, we just delete current row.
                // - Otherwise, it means that we are doing the alignment process in publish phase due to conflicts
                // during concurrent partial updates. And there exists another load which introduces a row with
                // the same keys and larger sequence column value published successfully after the commit phase
                // of the current load.
                //     - If the columns we update include sequence column, we should delete the current row becase the
                //       partial update on the current row has been `overwritten` by the previous one with larger sequence
                //       column value.
                //     - Otherwise, we should combine the values of the missing columns in the previous row and the values
                //       of the including columns in the current row into a new row.
                delete_bitmap->add({rowset_id, seg->id(), DeleteBitmap::TEMP_VERSION_COMMON},
                                   row_id);
                continue;
            }
            if (is_partial_update && rowset_writer != nullptr) {
                // In publish version, record rows to be deleted for concurrent update
                // For example, if version 5 and 6 update a row, but version 6 only see
                // version 4 when write, and when publish version, version 5's value will
                // be marked as deleted and it's update is losed.
                // So here we should read version 5's columns and build a new row, which is
                // consists of version 6's update columns and version 5's origin columns
                // here we build 2 read plan for ori values and update values
                prepare_to_read(loc, pos, &read_plan_ori);
                prepare_to_read(RowLocation {rowset_id, seg->id(), row_id}, pos, &read_plan_update);
                rsid_to_rowset[rowset_find->rowset_id()] = rowset_find;
                ++pos;
                // delete bitmap will be calculate when memtable flush and
                // publish. The two stages may see different versions.
                // When there is sequence column, the currently imported data
                // of rowset may be marked for deletion at memtablet flush or
                // publish because the seq column is smaller than the previous
                // rowset.
                // just set 0 as a unified temporary version number, and update to
                // the real version number later.
                delete_bitmap->add(
                        {loc.rowset_id, loc.segment_id, DeleteBitmap::TEMP_VERSION_COMMON},
                        loc.row_id);
                delete_bitmap->add({rowset_id, seg->id(), DeleteBitmap::TEMP_VERSION_COMMON},
                                   row_id);
                continue;
            }
            // when st = ok
            delete_bitmap->add({loc.rowset_id, loc.segment_id, DeleteBitmap::TEMP_VERSION_COMMON},
                               loc.row_id);
        }
        remaining -= num_read;
    }
    // DCHECK_EQ(total, row_id) << "segment total rows: " << total << " row_id:" << row_id;

    if (config::enable_merge_on_write_correctness_check) {
        RowsetIdUnorderedSet rowsetids;
        for (const auto& rowset : specified_rowsets) {
            rowsetids.emplace(rowset->rowset_id());
            LOG(INFO) << "[tabletID:" << tablet_id() << "]"
                      << "[add_sentinel_mark_to_delete_bitmap][end_version:" << end_version << "]"
                      << "add:" << rowset->rowset_id();
        }
        add_sentinel_mark_to_delete_bitmap(delete_bitmap.get(), rowsetids);
    }

    if (pos > 0) {
        auto partial_update_info = rowset_writer->get_partial_update_info();
        DCHECK(partial_update_info);
        RETURN_IF_ERROR(generate_new_block_for_partial_update(
                rowset_schema, partial_update_info->missing_cids, partial_update_info->update_cids,
                read_plan_ori, read_plan_update, rsid_to_rowset, &block));
        sort_block(block, ordered_block);
        RETURN_IF_ERROR(rowset_writer->flush_single_block(&ordered_block));
    }
    LOG(INFO) << "calc segment delete bitmap, tablet: " << tablet_id() << " rowset: " << rowset_id
              << " seg_id: " << seg->id() << " dummy_version: " << end_version + 1
              << " rows: " << seg->num_rows()
              << " bitmap num: " << delete_bitmap->delete_bitmap.size()
              << " cost: " << watch.get_elapse_time_us() << "(us)";
    return Status::OK();
}

void BaseTablet::sort_block(vectorized::Block& in_block, vectorized::Block& output_block) {
    vectorized::MutableBlock mutable_input_block =
            vectorized::MutableBlock::build_mutable_block(&in_block);
    vectorized::MutableBlock mutable_output_block =
            vectorized::MutableBlock::build_mutable_block(&output_block);

    std::vector<RowInBlock*> _row_in_blocks;
    _row_in_blocks.reserve(in_block.rows());

    std::shared_ptr<RowInBlockComparator> vec_row_comparator =
            std::make_shared<RowInBlockComparator>(_tablet_meta->tablet_schema().get());
    vec_row_comparator->set_block(&mutable_input_block);

    std::vector<RowInBlock*> row_in_blocks;
    DCHECK(in_block.rows() <= std::numeric_limits<int>::max());
    row_in_blocks.reserve(in_block.rows());
    for (size_t i = 0; i < in_block.rows(); ++i) {
        row_in_blocks.emplace_back(new RowInBlock {i});
    }
    std::sort(row_in_blocks.begin(), row_in_blocks.end(),
              [&](const RowInBlock* l, const RowInBlock* r) -> bool {
                  auto value = (*vec_row_comparator)(l, r);
                  DCHECK(value != 0) << "value equel when sort block, l_pos: " << l->_row_pos
                                     << " r_pos: " << r->_row_pos;
                  return value < 0;
              });
    std::vector<uint32_t> row_pos_vec;
    row_pos_vec.reserve(in_block.rows());
    for (auto* block : row_in_blocks) {
        row_pos_vec.emplace_back(block->_row_pos);
    }
    mutable_output_block.add_rows(&in_block, row_pos_vec.data(),
                                  row_pos_vec.data() + in_block.rows());
}

// fetch value by row column
Status BaseTablet::fetch_value_through_row_column(RowsetSharedPtr input_rowset,
                                                  const TabletSchema& tablet_schema, uint32_t segid,
                                                  const std::vector<uint32_t>& rowids,
                                                  const std::vector<uint32_t>& cids,
                                                  vectorized::Block& block) {
    MonotonicStopWatch watch;
    watch.start();
    Defer _defer([&]() {
        LOG_EVERY_N(INFO, 500) << "fetch_value_by_rowids, cost(us):" << watch.elapsed_time() / 1000
                               << ", row_batch_size:" << rowids.size();
    });

    BetaRowsetSharedPtr rowset = std::static_pointer_cast<BetaRowset>(input_rowset);
    CHECK(rowset);
    CHECK(tablet_schema.store_row_column());
    SegmentCacheHandle segment_cache_handle;
    std::unique_ptr<segment_v2::ColumnIterator> column_iterator;
    OlapReaderStatistics stats;
    RETURN_IF_ERROR(_get_segment_column_iterator(rowset, segid,
                                                 tablet_schema.column(BeConsts::ROW_STORE_COL),
                                                 &segment_cache_handle, &column_iterator, &stats));
    // get and parse tuple row
    vectorized::MutableColumnPtr column_ptr = vectorized::ColumnString::create();
    RETURN_IF_ERROR(column_iterator->read_by_rowids(rowids.data(), rowids.size(), column_ptr));
    assert(column_ptr->size() == rowids.size());
    auto* string_column = static_cast<vectorized::ColumnString*>(column_ptr.get());
    vectorized::DataTypeSerDeSPtrs serdes;
    serdes.resize(cids.size());
    std::unordered_map<uint32_t, uint32_t> col_uid_to_idx;
    std::vector<std::string> default_values;
    default_values.resize(cids.size());
    for (int i = 0; i < cids.size(); ++i) {
        const TabletColumn& column = tablet_schema.column(cids[i]);
        vectorized::DataTypePtr type =
                vectorized::DataTypeFactory::instance().create_data_type(column);
        col_uid_to_idx[column.unique_id()] = i;
        default_values[i] = column.default_value();
        serdes[i] = type->get_serde();
    }
    vectorized::JsonbSerializeUtil::jsonb_to_block(serdes, *string_column, col_uid_to_idx, block,
                                                   default_values);
    return Status::OK();
}

Status BaseTablet::fetch_value_by_rowids(RowsetSharedPtr input_rowset, uint32_t segid,
                                         const std::vector<uint32_t>& rowids,
                                         const TabletColumn& tablet_column,
                                         vectorized::MutableColumnPtr& dst) {
    MonotonicStopWatch watch;
    watch.start();
    Defer _defer([&]() {
        LOG_EVERY_N(INFO, 500) << "fetch_value_by_rowids, cost(us):" << watch.elapsed_time() / 1000
                               << ", row_batch_size:" << rowids.size();
    });

    // read row data
    BetaRowsetSharedPtr rowset = std::static_pointer_cast<BetaRowset>(input_rowset);
    CHECK(rowset);
    SegmentCacheHandle segment_cache_handle;
    std::unique_ptr<segment_v2::ColumnIterator> column_iterator;
    OlapReaderStatistics stats;
    RETURN_IF_ERROR(_get_segment_column_iterator(rowset, segid, tablet_column,
                                                 &segment_cache_handle, &column_iterator, &stats));
    RETURN_IF_ERROR(column_iterator->read_by_rowids(rowids.data(), rowids.size(), dst));
    return Status::OK();
}

Status BaseTablet::generate_new_block_for_partial_update(
        TabletSchemaSPtr rowset_schema, const std::vector<uint32>& missing_cids,
        const std::vector<uint32>& update_cids, const PartialUpdateReadPlan& read_plan_ori,
        const PartialUpdateReadPlan& read_plan_update,
        const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
        vectorized::Block* output_block) {
    // do partial update related works
    // 1. read columns by read plan
    // 2. generate new block
    // 3. write a new segment and modify rowset meta
    // 4. mark current keys deleted
    CHECK(output_block);
    auto full_mutable_columns = output_block->mutate_columns();
    auto old_block = rowset_schema->create_block_by_cids(missing_cids);
    auto update_block = rowset_schema->create_block_by_cids(update_cids);

    std::map<uint32_t, uint32_t> read_index_old;
    RETURN_IF_ERROR(read_columns_by_plan(rowset_schema, missing_cids, read_plan_ori, rsid_to_rowset,
                                         old_block, &read_index_old));

    std::map<uint32_t, uint32_t> read_index_update;
    RETURN_IF_ERROR(read_columns_by_plan(rowset_schema, update_cids, read_plan_update,
                                         rsid_to_rowset, update_block, &read_index_update));

    // build full block
    CHECK(read_index_old.size() == read_index_update.size());
    for (auto i = 0; i < missing_cids.size(); ++i) {
        for (auto idx = 0; idx < read_index_old.size(); ++idx) {
            full_mutable_columns[missing_cids[i]]->insert_from(
                    *old_block.get_columns_with_type_and_name()[i].column.get(),
                    read_index_old[idx]);
        }
    }
    for (auto i = 0; i < update_cids.size(); ++i) {
        for (auto idx = 0; idx < read_index_update.size(); ++idx) {
            full_mutable_columns[update_cids[i]]->insert_from(
                    *update_block.get_columns_with_type_and_name()[i].column.get(),
                    read_index_update[idx]);
        }
    }
    output_block->set_columns(std::move(full_mutable_columns));
    VLOG_DEBUG << "full block when publish: " << output_block->dump_data();
    return Status::OK();
}

Status BaseTablet::commit_phase_update_delete_bitmap(
        const BaseTabletSPtr& tablet, const RowsetSharedPtr& rowset,
        RowsetIdUnorderedSet& pre_rowset_ids, DeleteBitmapPtr delete_bitmap,
        const std::vector<segment_v2::SegmentSharedPtr>& segments, int64_t txn_id,
        CalcDeleteBitmapToken* token, RowsetWriter* rowset_writer) {
    SCOPED_BVAR_LATENCY(g_tablet_commit_phase_update_delete_bitmap_latency);
    RowsetIdUnorderedSet cur_rowset_ids;
    RowsetIdUnorderedSet rowset_ids_to_add;
    RowsetIdUnorderedSet rowset_ids_to_del;
    int64_t cur_version;

    std::vector<RowsetSharedPtr> specified_rowsets;
    {
        std::shared_lock meta_rlock(tablet->_meta_lock);
        cur_version = tablet->max_version_unlocked();
        RETURN_IF_ERROR(tablet->get_all_rs_id_unlocked(cur_version, &cur_rowset_ids));
        _rowset_ids_difference(cur_rowset_ids, pre_rowset_ids, &rowset_ids_to_add,
                               &rowset_ids_to_del);
        specified_rowsets = tablet->get_rowset_by_ids(&rowset_ids_to_add);
    }
    for (const auto& to_del : rowset_ids_to_del) {
        delete_bitmap->remove({to_del, 0, 0}, {to_del, UINT32_MAX, INT64_MAX});
    }

    RETURN_IF_ERROR(calc_delete_bitmap(tablet, rowset, segments, specified_rowsets, delete_bitmap,
                                       cur_version, token, rowset_writer));
    size_t total_rows = std::accumulate(
            segments.begin(), segments.end(), 0,
            [](size_t sum, const segment_v2::SegmentSharedPtr& s) { return sum += s->num_rows(); });
    LOG(INFO) << "[Before Commit] construct delete bitmap tablet: " << tablet->tablet_id()
              << ", rowset_ids to add: " << rowset_ids_to_add.size()
              << ", rowset_ids to del: " << rowset_ids_to_del.size()
              << ", cur max_version: " << cur_version << ", transaction_id: " << txn_id
              << ", total rows: " << total_rows;
    pre_rowset_ids = cur_rowset_ids;
    return Status::OK();
}

void BaseTablet::add_sentinel_mark_to_delete_bitmap(DeleteBitmap* delete_bitmap,
                                                    const RowsetIdUnorderedSet& rowsetids) {
    for (const auto& rowsetid : rowsetids) {
        delete_bitmap->add(
                {rowsetid, DeleteBitmap::INVALID_SEGMENT_ID, DeleteBitmap::TEMP_VERSION_COMMON},
                DeleteBitmap::ROWSET_SENTINEL_MARK);
    }
}

void BaseTablet::_rowset_ids_difference(const RowsetIdUnorderedSet& cur,
                                        const RowsetIdUnorderedSet& pre,
                                        RowsetIdUnorderedSet* to_add,
                                        RowsetIdUnorderedSet* to_del) {
    for (const auto& id : cur) {
        if (pre.find(id) == pre.end()) {
            to_add->insert(id);
        }
    }
    for (const auto& id : pre) {
        if (cur.find(id) == cur.end()) {
            to_del->insert(id);
        }
    }
}

Status BaseTablet::_capture_consistent_rowsets_unlocked(
        const std::vector<Version>& version_path, std::vector<RowsetSharedPtr>* rowsets) const {
    DCHECK(rowsets != nullptr);
    rowsets->reserve(version_path.size());
    for (const auto& version : version_path) {
        bool is_find = false;
        do {
            auto it = _rs_version_map.find(version);
            if (it != _rs_version_map.end()) {
                is_find = true;
                rowsets->push_back(it->second);
                break;
            }

            auto it_expired = _stale_rs_version_map.find(version);
            if (it_expired != _stale_rs_version_map.end()) {
                is_find = true;
                rowsets->push_back(it_expired->second);
                break;
            }
        } while (false);

        if (!is_find) {
            return Status::Error<CAPTURE_ROWSET_ERROR>(
                    "fail to find Rowset for version. tablet={}, version={}", tablet_id(),
                    version.to_string());
        }
    }
    return Status::OK();
}

} // namespace doris
