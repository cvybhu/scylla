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
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015-present ScyllaDB
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

#include "term.hh"

namespace cql3 {
    namespace rewrite {
        // TODO: This is not very good, maybe column name?
        bool bound_value::operator==(const bound_value& other) const {
            return bind_index == other.bind_index && receiver.get() == other.receiver.get(); 
        }

        // TODO: This is not very good, maybe column name?
        bool bound_value::operator<(const bound_value& other) const {
            if (bind_index != other.bind_index) {
                return bind_index < other.bind_index;
            }

            return receiver.get() < other.receiver.get();
        }

        bool delayed_tuple::operator==(const delayed_tuple& other) const {
            return elements == other.elements;
        }

        bool delayed_tuple::operator<(const delayed_tuple& other) const {
            return elements < other.elements;
        }

        bool delayed_list::operator==(const delayed_list& other) const {
            return elements == other.elements;
        }

        bool delayed_list::operator<(const delayed_list& other) const {
            return elements < other.elements;
        }

        bool delayed_set::operator==(const delayed_set& other) const {
            return elements == other.elements;
        }

        bool delayed_set::operator<(const delayed_set& other) const {
            return elements < other.elements;
        }

        bool delayed_map::operator==(const delayed_map& other) const {
            return elements == other.elements;
        }
        bool delayed_map::operator<(const delayed_map& other) const {
            return elements < other.elements;
        }

        // TODO: This is not very good, maybe function name?
        bool delayed_function::operator==(const delayed_function& other) const {
            return function.get() == other.function.get() && arguments == other.arguments;
        }

        // TODO: This is not very good, maybe function name?
        bool delayed_function::operator<(const delayed_function& other) const {
            if (function.get() != other.function.get()) {
                return std::less{}(function.get(), other.function.get());
            }

            return arguments < other.arguments;
        }

        bool delayed_user_type::operator==(const delayed_user_type& other) const {
            return field_values == other.field_values;
        }

        bool delayed_user_type::operator<(const delayed_user_type& other) const {
            return field_values < other.field_values;
        }

        term to_new_term(const ::shared_ptr<cql3::term>& old_term) {
            if (old_term.get() == nullptr) {
                return term(cql_value(null_value{}));
            }

            return old_term->to_new_term();
        }
    }
}