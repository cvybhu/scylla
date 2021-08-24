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

#pragma once

#include "cql3/abstract_marker.hh"
#include "column_specification.hh"
#include "term.hh"
#include "column_identifier.hh"
#include "operation.hh"
#include "to_string.hh"

namespace cql3 {

/**
 * Static helper methods and classes for user types.
 */
class user_types {
    user_types() = delete;
public:
    static lw_shared_ptr<column_specification> field_spec_of(const column_specification& column, size_t field);

    class value : public multi_item_terminal {
        std::vector<managed_bytes_opt> _elements;
        data_type _my_type;
    public:
        explicit value(std::vector<managed_bytes_opt>, data_type my_type);

        static value from_serialized(const raw_value_view&, const user_type_impl&);

        virtual cql3::raw_value get(const query_options&) override;
        const std::vector<managed_bytes_opt>& get_elements() const;
        virtual std::vector<managed_bytes_opt> copy_elements() const override;
        virtual sstring to_string() const override;
        virtual data_type get_value_type() const;
    };

    // Same purpose than Lists.DelayedValue, except we do handle bind marker in that case
    class delayed_value : public non_terminal {
        user_type _type;
        std::vector<shared_ptr<term>> _values;
    public:
        delayed_value(user_type type, std::vector<shared_ptr<term>> values);
        virtual bool contains_bind_marker() const override;
        virtual void fill_prepare_context(prepare_context& ctx) const;
    private:
        std::vector<managed_bytes_opt> bind_internal(const query_options& options);
    public:
        virtual shared_ptr<terminal> bind(const query_options& options) override;
        virtual cql3::raw_value_view bind_and_get(const query_options& options) override;
    };

    class marker : public abstract_marker {
    public:
        marker(int32_t bind_index, lw_shared_ptr<column_specification> receiver)
            : abstract_marker{bind_index, std::move(receiver)}
        {
            assert(_receiver->type->is_user_type());
        }

        virtual shared_ptr<terminal> bind(const query_options& options) override;
    };

    class setter : public operation {
    public:
        using operation::operation;

        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
        static void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const column_definition& column, ::shared_ptr<terminal> value);
    };

    class setter_by_field : public operation {
        size_t _field_idx;
    public:
        setter_by_field(const column_definition& column, size_t field_idx, shared_ptr<term> t)
            : operation(column, std::move(t)), _field_idx(field_idx) {
        }

        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
    };

    class deleter_by_field : public operation {
        size_t _field_idx;
    public:
        deleter_by_field(const column_definition& column, size_t field_idx)
            : operation(column, nullptr), _field_idx(field_idx) {
        }

        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
    };
};

}
