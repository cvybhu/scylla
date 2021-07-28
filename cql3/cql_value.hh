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

#include "cql3/values.hh"
#include <variant>
#include <vector>
#include <map>

namespace cql3 {
    struct cql_value;

    struct simple_value {
        cql3::raw_value value;

        bool operator==(const simple_value& other) const = default;
    };

    struct tuple_value {
        std::vector<cql_value> elements;

        bool operator==(const tuple_value& other) const;

        cql3::raw_value to_raw_value() const {
            throw std::runtime_error("tuple_value::to_raw_value is not implemented");
        }

        std::vector<managed_bytes_opt> get_serialized_elements() const;
    };

    struct list_value {
        std::vector<cql_value> elements;

        bool operator==(const list_value& other) const;

        cql3::raw_value to_raw_value() const {
            throw std::runtime_error("list_value::to_raw_value is not implemented");
        }

        std::vector<managed_bytes_opt> get_serialized_elements() const;

        managed_bytes get_with_protocol_version(cql_serialization_format) const {
            throw std::runtime_error("list_value::get_with_protocol_version is not implemented");
        }
    };

    struct set_value {
        std::vector<cql_value> elements;

        bool operator==(const set_value& other) const;
    };

    struct map_value {
        std::map<cql_value, cql_value> elements;

        bool operator==(const map_value& other) const;
    };

    struct user_type_value {
        std::map<sstring, cql_value> fields;

        bool operator==(const user_type_value& other) const;
    };

    struct cql_value {
        std::variant<null_value,
                     unset_value,
                     simple_value,
                     tuple_value,
                     list_value,
                     set_value,
                     map_value,
                     user_type_value> value;

        template<class T>
        cql_value(T&& val) : value(std::forward<T>(val)) {}

        bool operator==(const cql_value& other) const = default;

        cql3::raw_value to_raw_value() const {
            throw std::runtime_error("cql_value::to_raw_value is not implemented");
        }
    };
}