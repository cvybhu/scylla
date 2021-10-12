/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "cql3/attributes.hh"
#include "cql3/column_identifier.hh"

namespace cql3 {

std::unique_ptr<attributes> attributes::none() {
    return std::unique_ptr<attributes>{new attributes{{}, {}, {}}};
}

attributes::attributes(std::optional<cql3::expr::expression>&& timestamp,
                       std::optional<cql3::expr::expression>&& time_to_live,
                       std::optional<cql3::expr::expression>&& timeout)
    : _timestamp{std::move(timestamp)}
    , _time_to_live{std::move(time_to_live)}
    , _timeout{std::move(timeout)}
{ }

bool attributes::is_timestamp_set() const {
    return _timestamp.has_value();
}

bool attributes::is_time_to_live_set() const {
    return _time_to_live.has_value();
}

bool attributes::is_timeout_set() const {
    return _timeout.has_value();
}

int64_t attributes::get_timestamp(int64_t now, const query_options& options) {
    if (!_timestamp.has_value()) {
        return now;
    }

    expr::constant tval = expr::evaluate(*_timestamp, options);
    if (tval.is_null()) {
        throw exceptions::invalid_request_exception("Invalid null value of timestamp");
    }
    if (tval.is_unset_value()) {
        return now;
    }
    try {
        return tval.view().validate_and_deserialize<int64_t>(*long_type, cql_serialization_format::internal());
    } catch (marshal_exception& e) {
        throw exceptions::invalid_request_exception("Invalid timestamp value");
    }
}

int32_t attributes::get_time_to_live(const query_options& options) {
    if (!_time_to_live.has_value())
        return 0;

    expr::constant tval = expr::evaluate(*_time_to_live, options);
    if (tval.is_null()) {
        throw exceptions::invalid_request_exception("Invalid null value of TTL");
    }
    if (tval.is_unset_value()) {
        return 0;
    }

    int32_t ttl;
    try {
        ttl = tval.view().validate_and_deserialize<int32_t>(*int32_type, cql_serialization_format::internal());
    }
    catch (marshal_exception& e) {
        throw exceptions::invalid_request_exception("Invalid TTL value");
    }

    if (ttl < 0) {
        throw exceptions::invalid_request_exception("A TTL must be greater or equal to 0");
    }

    if (ttl > max_ttl.count()) {
        throw exceptions::invalid_request_exception("ttl is too large. requested (" + std::to_string(ttl) +
            ") maximum (" + std::to_string(max_ttl.count()) + ")");
    }

    return ttl;
}


db::timeout_clock::duration attributes::get_timeout(const query_options& options) const {
    if (!_timeout.has_value()) {
        throw exceptions::invalid_request_exception("Timeout value not set");
    }

    expr::constant timeout = expr::evaluate(*_timeout, options);
    if (timeout.is_null() || timeout.is_unset_value()) {
        throw exceptions::invalid_request_exception("Timeout value cannot be unset/null");
    }
    cql_duration duration = timeout.view().deserialize<cql_duration>(*duration_type);
    if (duration.months || duration.days) {
        throw exceptions::invalid_request_exception("Timeout values cannot be expressed in days/months");
    }
    if (duration.nanoseconds % 1'000'000 != 0) {
        throw exceptions::invalid_request_exception("Timeout values cannot have granularity finer than milliseconds");
    }
    if (duration.nanoseconds < 0) {
        throw exceptions::invalid_request_exception("Timeout values must be non-negative");
    }
    return std::chrono::duration_cast<db::timeout_clock::duration>(std::chrono::nanoseconds(duration.nanoseconds));
}

void attributes::fill_prepare_context(prepare_context& ctx) {
    if (_timestamp.has_value()) {
        expr::fill_prepare_context(*_timestamp, ctx);
    }
    if (_time_to_live.has_value()) {
        expr::fill_prepare_context(*_time_to_live, ctx);
    }
    if (_timeout.has_value()) {
        expr::fill_prepare_context(*_timeout, ctx);
    }
}

std::unique_ptr<attributes> attributes::raw::prepare(database& db, const sstring& ks_name, const sstring& cf_name) const {
    std::optional<expr::expression> ts, ttl, to;

    if (timestamp.has_value()) {
        ts = expr::to_expression(prepare_term(*timestamp, db, ks_name, timestamp_receiver(ks_name, cf_name)));
    }

    if (time_to_live.has_value()) {
        ttl = expr::to_expression(prepare_term(*time_to_live, db, ks_name, time_to_live_receiver(ks_name, cf_name)));
    }

    if (timeout.has_value()) {
        to = expr::to_expression(prepare_term(*timeout, db, ks_name, timeout_receiver(ks_name, cf_name)));
    }

    return std::unique_ptr<attributes>{new attributes{std::move(ts), std::move(ttl), std::move(to)}};
}

lw_shared_ptr<column_specification> attributes::raw::timestamp_receiver(const sstring& ks_name, const sstring& cf_name) const {
    return make_lw_shared<column_specification>(ks_name, cf_name, ::make_shared<column_identifier>("[timestamp]", true), data_type_for<int64_t>());
}

lw_shared_ptr<column_specification> attributes::raw::time_to_live_receiver(const sstring& ks_name, const sstring& cf_name) const {
    return make_lw_shared<column_specification>(ks_name, cf_name, ::make_shared<column_identifier>("[ttl]", true), data_type_for<int32_t>());
}

lw_shared_ptr<column_specification> attributes::raw::timeout_receiver(const sstring& ks_name, const sstring& cf_name) const {
    return make_lw_shared<column_specification>(ks_name, cf_name, ::make_shared<column_identifier>("[timeout]", true), duration_type);
}

}
