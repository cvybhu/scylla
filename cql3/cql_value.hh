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

#pragma once

#include "cql3/values.hh"

namespace cql3 {
    struct bool_value {
        bool value;

        bool_value(bytes_view serialized_bytes);
    };

    struct int8_value {
        int8_t value;

        int8_value(bytes_view serialized_bytes);
    };

    struct int16_value {
        int16_t value;

        int16_value(bytes_view serialized_bytes);
    };

    struct int32_value {
        int32_t value;

        int32_value(bytes_view serialized_bytes);
    };

    struct int64_value {
        int64_t value;

        int64_value(bytes_view serialized_bytes);
    };

    struct counter_value {
        int64_t value;

        counter_value(bytes_view serialized_bytes);
    };

    struct varint_value {
        managed_bytes value;

        varint_value(bytes_view serialized_bytes);
    };

    struct float_value {
        float value;

        float_value(bytes_view serialized_bytes);
    };

    struct double_value {
        double value;

        double_value(bytes_view serialized_bytes);
    };

    struct decimal_value {
        managed_bytes value;

        decimal_value(bytes_view serialized_bytes);
    };

    struct ascii_value {
        managed_bytes value;

        ascii_value(bytes_view serialized_bytes);
    };

    struct utf8_value {
        managed_bytes value;

        utf8_value(bytes_view serialized_bytes);
    };

    struct date_value {
        managed_bytes value;

        date_value(bytes_view serialized_bytes);
    };

    struct simple_date_value {
        managed_bytes value;

        simple_date_value(bytes_view serialized_bytes);
    };

    struct duration_value {
        managed_bytes value;

        duration_value(bytes_view serialized_bytes);
    };

    struct time_value {
        managed_bytes value;

        time_value(bytes_view serialized_bytes);
    };

    struct timestamp_value {
        managed_bytes value;

        timestamp_value(bytes_view serialized_bytes);
    };

    struct timeuuid_value {
        managed_bytes value;

        timeuuid_value(bytes_view serialized_bytes);
    };

    struct blob_value {
        managed_bytes value;

        blob_value(bytes_view serialized_bytes);
    };

    struct inet_value {
        managed_bytes value;

        inet_value(bytes_view serialized_bytes);
    };

    struct uuid_value {
        managed_bytes value;

        uuid_value(bytes_view serialized_bytes);
    };

    struct tuple_value {
        // TODO: Change to vector<cql_value>
        std::vector<std::variant<managed_bytes, null_value>> elements;
        // Not every element in a tuple has a type, because in some cases the tuple_type_impl is empty.
        // TODO: Find the types in these cases.
        std::vector<std::optional<data_type>> elements_types;
    };

    struct list_value {
        // TODO: change managed_bytes to non_null_cql_value.
        std::vector<managed_bytes> elements;
        data_type elements_type;
    };

    struct set_value {
        // TODO: change managed_bytes to non_null_cql_value.
        std::set<managed_bytes, serialized_compare> elements;
        data_type elements_type;
    };

    struct map_value {
        // TODO: change managed_bytes to non_null_cql_value.
        std::map<managed_bytes, managed_bytes, serialized_compare> elements; 
        data_type keys_type;
        data_type values_type;
    };

    struct user_type_value {
        // TODO: change to vector<cql_value>
        std::vector<std::variant<managed_bytes, null_value>> field_values;
        std::vector<data_type> field_values_types;
    };

    using cql_value = std::variant<
        unset_value,
        null_value,
        bool_value,
        int8_value,
        int16_value,
        int32_value,
        int64_value,
        counter_value,
        varint_value,
        float_value,
        double_value,
        decimal_value,
        ascii_value,
        utf8_value,
        date_value,
        simple_date_value,
        duration_value,
        time_value,
        timestamp_value,
        timeuuid_value,
        blob_value,
        inet_value,
        uuid_value,
        tuple_value,
        list_value,
        set_value,
        map_value,
        user_type_value>;

    // A cql_value that is ordered in reverse order
    struct reversed_cql_value {
        cql_value value;
    };

    using ordered_cql_value = std::variant<cql_value, reversed_cql_value>;
}