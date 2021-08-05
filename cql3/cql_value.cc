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
 * Copyright (C) 2021-present ScyllaDB
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

#include "cql_value.hh"
namespace cql3 {

bool_value::bool_value(bytes_view serialized_bytes) {
    if (serialized_bytes.size() != 1) {
        throw std::runtime_error("bool_value(serialized_bytes) bytes size is not 1");
    }

    value = serialized_bytes[0] != 0;
}

int8_value::int8_value(bytes_view serialized_bytes) {
    static_assert(sizeof(value) == 1);

    if (serialized_bytes.size() != sizeof(value)) {
        throw std::runtime_error(fmt::format("int8_value(serialized_bytes) bad bytes size expected: {}, found: {}",
                                 sizeof(value), serialized_bytes.size()));
    }

    memcpy(&value, &serialized_bytes[0], sizeof(value));
}

int16_value::int16_value(bytes_view serialized_bytes) {
    static_assert(sizeof(value) == 2);

    if (serialized_bytes.size() != sizeof(value)) {
        throw std::runtime_error(fmt::format("int16_value(serialized_bytes) bad bytes size expected: {}, found: {}",
                                 sizeof(value), serialized_bytes.size()));
    }

    memcpy(&value, &serialized_bytes[0], sizeof(value));
    value = be_to_cpu(value);
}

int32_value::int32_value(bytes_view serialized_bytes) {
    static_assert(sizeof(value) == 4);

    if (serialized_bytes.size() != sizeof(value)) {
        throw std::runtime_error(fmt::format("int32_value(serialized_bytes) bad bytes size expected: {}, found: {}",
                                 sizeof(value), serialized_bytes.size()));
    }

    memcpy(&value, &serialized_bytes[0], sizeof(value));
    value = be_to_cpu(value);
}

int64_value::int64_value(bytes_view serialized_bytes) {
    static_assert(sizeof(value) == 8);

    if (serialized_bytes.size() != sizeof(value)) {
        throw std::runtime_error(fmt::format("int64_value(serialized_bytes) bad bytes size expected: {}, found: {}",
                                 sizeof(value), serialized_bytes.size()));
    }

    memcpy(&value, &serialized_bytes[0], sizeof(value));
    value = be_to_cpu(value);
}

counter_value::counter_value(bytes_view serialized_bytes) {
    static_assert(sizeof(value) == 8);

    if (serialized_bytes.size() != sizeof(value)) {
        throw std::runtime_error(fmt::format("counter_value(serialized_bytes) bad bytes size expected: {}, found: {}",
                                 sizeof(value), serialized_bytes.size()));
    }

    memcpy(&value, &serialized_bytes[0], sizeof(value));
    value = be_to_cpu(value);
}

varint_value::varint_value(bytes_view serialized_bytes) : value(serialized_bytes) {}

float_value::float_value(bytes_view serialized_bytes) {
    static_assert(sizeof(value) == 4);

    if (serialized_bytes.size() != sizeof(value)) {
        throw std::runtime_error(fmt::format("float_value(serialized_bytes) bad bytes size expected: {}, found: {}",
                                 sizeof(value), serialized_bytes.size()));
    }

    int32_t value_int;
    static_assert(sizeof(value_int) == sizeof(value));
    memcpy(&value_int, &serialized_bytes[0], sizeof(value));
    value_int = be_to_cpu(value_int);

    memcpy(&value, &value_int, sizeof(value));
}

double_value::double_value(bytes_view serialized_bytes) {
    static_assert(sizeof(value) == 8);

    if (serialized_bytes.size() != sizeof(value)) {
        throw std::runtime_error(fmt::format("double_value(serialized_bytes) bad bytes size expected: {}, found: {}",
                                 sizeof(value), serialized_bytes.size()));
    }

    int64_t value_int;
    static_assert(sizeof(value_int) == sizeof(value));
    memcpy(&value_int, &serialized_bytes[0], sizeof(value));
    value_int = be_to_cpu(value_int);

    memcpy(&value, &value_int, sizeof(value));
}

decimal_value::decimal_value(bytes_view serialized_bytes) : value(serialized_bytes) {}

ascii_value::ascii_value(bytes_view serialized_bytes) : value(serialized_bytes) {}

utf8_value::utf8_value(bytes_view serialized_bytes) : value(serialized_bytes) {}

date_value::date_value(bytes_view serialized_bytes) : value(serialized_bytes) {}

simple_date_value::simple_date_value(bytes_view serialized_bytes) : value(serialized_bytes) {}

duration_value::duration_value(bytes_view serialized_bytes) : value(serialized_bytes) {}

time_value::time_value(bytes_view serialized_bytes) : value(serialized_bytes) {}

timestamp_value::timestamp_value(bytes_view serialized_bytes) : value(serialized_bytes) {}

timeuuid_value::timeuuid_value(bytes_view serialized_bytes) : value(serialized_bytes) {}

blob_value::blob_value(bytes_view serialized_bytes) : value(serialized_bytes) {}

inet_value::inet_value(bytes_view serialized_bytes) : value(serialized_bytes) {}

uuid_value::uuid_value(bytes_view serialized_bytes) : value(serialized_bytes) {}

ordered_cql_value reverse_if_needed(cql_value&& value, bool should_reverse) {
    if (should_reverse) {
        return ordered_cql_value(reversed_cql_value{std::move(value)});
    } else {
        return ordered_cql_value(std::move(value));
    }
}
}
