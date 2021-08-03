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

    class literal : public term::raw {
    public:
        using elements_map_type = std::unordered_map<column_identifier, shared_ptr<term::raw>>;
        elements_map_type _entries;

        literal(elements_map_type entries);
        virtual shared_ptr<term> prepare(database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) const override;
    private:
        void validate_assignable_to(database& db, const sstring& keyspace, const column_specification& receiver) const;
    public:
        virtual assignment_testable::test_result test_assignment(database& db, const sstring& keyspace, const column_specification& receiver) const override;
        virtual sstring assignment_testable_source_context() const override;
        virtual sstring to_string() const override;
    };

    class value : public multi_item_terminal {
        std::vector<managed_bytes_opt> _elements;
        std::vector<data_type> _element_types;
    public:
        explicit value(std::vector<managed_bytes_opt>, std::vector<data_type> element_types);

        static value from_serialized(const raw_value_view&, const user_type_impl&);

        const std::vector<managed_bytes_opt>& get_elements() const;
        virtual std::vector<managed_bytes_opt> copy_elements() const override;
        virtual sstring to_string() const override;

        virtual rewrite::term to_new_term() const override {
            std::vector<cql_value> new_elements;
            new_elements.reserve(_elements.size());

            for (size_t i = 0; i < _elements.size(); i++) {
                if (_elements[i].has_value()) {
                    new_elements.emplace_back(serialized_value(to_bytes(*_elements[i]), _element_types[i]));
                } else {
                    new_elements.emplace_back(null_value{});
                }
            }
            
            return rewrite::term(cql_value(user_type_value{std::move(new_elements)}));
        };
    };

    // Same purpose than Lists.DelayedValue, except we do handle bind marker in that case
    class delayed_value : public non_terminal {
        user_type _type;
        std::vector<shared_ptr<term>> _values;
    public:
        delayed_value(user_type type, std::vector<shared_ptr<term>> values);
        virtual bool contains_bind_marker() const override;
        virtual void collect_marker_specification(variable_specifications& bound_names) const;
    private:
        std::vector<managed_bytes_opt> bind_internal(const query_options& options);
    public:
        virtual shared_ptr<terminal> bind(const query_options& options) override;
        virtual cql3::raw_value_view bind_and_get(const query_options& options) override;

        virtual rewrite::term to_new_term() const override {
            std::vector<rewrite::term> new_values;
            new_values.reserve(_values.size());

            for (const ::shared_ptr<term>& val : _values){
                new_values.emplace_back(rewrite::to_new_term(val));
            }

            return rewrite::term(rewrite::delayed_user_type{std::move(new_values)});
        };
    };

    class marker : public abstract_marker {
    public:
        marker(int32_t bind_index, lw_shared_ptr<column_specification> receiver)
            : abstract_marker{bind_index, std::move(receiver)}
        {
            assert(_receiver->type->is_user_type());
        }

        virtual shared_ptr<terminal> bind(const query_options& options) override;

        virtual rewrite::term to_new_term() const override {
            return rewrite::term(rewrite::delayed_cql_value(rewrite::bound_value{_bind_index, _receiver}));
        };
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
