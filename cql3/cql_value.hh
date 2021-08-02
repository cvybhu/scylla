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
 * Copyright (C) 2014-present ScyllaDB
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
#include <variant>
#include <vector>
#include <map>

namespace cql3 {
    // unset_value is defined in cql3/values.hh
    // null_value is defined in cql3/values.hh
    struct serialized_value;
    struct tuple_value;
    struct list_value;
    struct set_value;
    struct map_value;
    struct user_type_value;

    // CQL value represents a single data value occuring in CQL
    using cql_value = std::variant<unset_value,
                                   null_value,
                                   serialized_value,
                                   tuple_value,
                                   list_value,
                                   set_value,
                                   map_value,
                                   user_type_value>;

    // A value that has been already serialized to bytes
    // Not null or unset
    struct serialized_value {
        bytes data;
        std::optional<data_type> type;

        bool operator==(const serialized_value& other) const;
        bool operator<(const serialized_value& other) const;
    };

    struct tuple_value {
        std::vector<cql_value> elements;

        bool operator==(const tuple_value& other) const;
        bool operator<(const tuple_value& other) const;
    };

    struct list_value {
        std::vector<cql_value> elements;

        bool operator==(const list_value& other) const;
        bool operator<(const list_value& other) const;
    };

    struct set_value {
        std::set<cql_value> elements;

        bool operator==(const set_value& other) const;
        bool operator<(const set_value& other) const;
    };

    struct map_value {
        std::map<cql_value, cql_value> elements;

        bool operator==(const map_value& other) const;
        bool operator<(const map_value& other) const;
    };

    struct user_type_value {
        std::vector<cql_value> field_values;

        bool operator==(const user_type_value& other) const;
        bool operator<(const user_type_value& other) const;
    };

    cql3::raw_value to_raw_value(const cql_value&);

    cql3::raw_value to_raw_value(const unset_value&, cql_serialization_format);
    cql3::raw_value to_raw_value(const null_value&, cql_serialization_format);
    cql3::raw_value to_raw_value(const serialized_value&, cql_serialization_format);
    cql3::raw_value to_raw_value(const tuple_value&, cql_serialization_format);
    cql3::raw_value to_raw_value(const list_value&, cql_serialization_format);
    cql3::raw_value to_raw_value(const set_value&, cql_serialization_format);
    cql3::raw_value to_raw_value(const map_value&, cql_serialization_format);
    cql3::raw_value to_raw_value(const user_type_value&, cql_serialization_format);

    managed_bytes_opt to_managed_bytes_opt(const cql_value&, cql_serialization_format);
}
