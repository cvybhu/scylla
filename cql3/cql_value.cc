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

#include "cql_value.hh"
#include "utils/overloaded_functor.hh"

namespace cql3 {
    static const abstract_type* get_type(const serialized_value& a, const serialized_value& b) {
        const abstract_type* data_type = a.type.has_value() ? a.type->get() : nullptr;

        if (b.type.has_value() && std::less{}(b.type->get(), data_type)) {
            data_type = b.type->get();
        }

        return data_type;
    }

    // TODO: This is bad, find another way to do things when of not the same type
    bool serialized_value::operator==(const serialized_value& other) const {
        const abstract_type* data_type = get_type(*this, other);

        if (data_type != nullptr) {
            return data_type->equal(data, other.data);
        } else {
            return data < other.data;
        }
    }

    bool serialized_value::operator<(const serialized_value& other) const {
        const abstract_type* data_type = get_type(*this, other);

        if (data_type != nullptr) {
            return data_type->less(data, other.data);
        } else {
            return data < other.data;
        }
    }

    bool tuple_value::operator==(const tuple_value& other) const {
        return elements == other.elements;
    }

    bool tuple_value::operator<(const tuple_value& other) const {
        return elements < other.elements;
    }

    bool list_value::operator==(const list_value& other) const {
        return elements == other.elements;
    }

    bool list_value::operator<(const list_value& other) const {
        return elements < other.elements;
    }

    bool set_value::operator==(const set_value& other) const {
        return elements == other.elements;
    }

    bool set_value::operator<(const set_value& other) const {
        return elements < other.elements;
    }

    bool map_value::operator==(const map_value& other) const {
        return elements == other.elements;
    }

    bool map_value::operator<(const map_value& other) const {
        return elements < other.elements;
    }

    bool user_type_value::operator==(const user_type_value& other) const {
        return field_values == other.field_values;
    }

    bool user_type_value::operator<(const user_type_value& other) const {
        return field_values < other.field_values;
    }

    cql3::raw_value to_raw_value(const cql_value& cql_val) {
        return std::visit(overloaded_functor{[](const auto& val) {return to_raw_value(val);}}, cql_val);
    }

    cql3::raw_value to_raw_value(const unset_value&) {
        return cql3::raw_value::make_unset_value();
    }

    cql3::raw_value to_raw_value(const null_value&) {
        return cql3::raw_value::make_null();
    }

    cql3::raw_value to_raw_value(const serialized_value& val) {
        return cql3::raw_value::make_value(val.data);
    }

    cql3::raw_value to_raw_value(const tuple_value& val) {
        std::vector<managed_bytes_opt> serialized_elements;
        serialized_elements.reserve(val.elements.size());

        for (const cql_value& elem : val.elements) {
            serialized_elements.emplace_back(to_managed_bytes_opt(elem));
        }

        size_t size = 0;
        for (const managed_bytes_opt& serialized_elem : serialized_elements) {
            size += 4 + (serialized_elem ? serialized_elem->size() : 0);
        }
        managed_bytes ret = managed_bytes(managed_bytes::initialized_later(), size);
        managed_bytes_mutable_view out = managed_bytes_mutable_view(ret);
        for (const managed_bytes_opt& serialized_elem : serialized_elements) {
            if (serialized_elem.has_value()) {
                write<int32_t>(out, serialized_elem->size());
                write_fragmented(out, managed_bytes_view(*serialized_elem));
            } else {
                write<int32_t>(out, -1);
            }
        }
        
        return cql3::raw_value::make_value(ret);
    }

    cql3::raw_value to_raw_value(const list_value&) {
        throw std::runtime_error(fmt::format("{}:{} - Unimplemented!", __FILE__, __LINE__));
    }

    cql3::raw_value to_raw_value(const set_value&) {
        throw std::runtime_error(fmt::format("{}:{} - Unimplemented!", __FILE__, __LINE__));
    }

    cql3::raw_value to_raw_value(const map_value&) {
        throw std::runtime_error(fmt::format("{}:{} - Unimplemented!", __FILE__, __LINE__));
    }

    cql3::raw_value to_raw_value(const user_type_value&) {
        throw std::runtime_error(fmt::format("{}:{} - Unimplemented!", __FILE__, __LINE__));
    }

    managed_bytes_opt to_managed_bytes_opt(const cql_value& val) {
        if (val == cql_value(null_value{}) || val == cql_value(unset_value{})) {
            return std::nullopt;
        }

        return std::make_optional(managed_bytes(to_raw_value(val).to_bytes()));
    }
}