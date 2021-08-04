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

#pragma once

#include "cql3/abstract_marker.hh"
#include "cql3/update_parameters.hh"
#include "cql3/operation.hh"
#include "cql3/values.hh"
#include "cql3/term.hh"
#include "mutation.hh"
#include <seastar/core/shared_ptr.hh>

namespace cql3 {

/**
 * Static helper methods and classes for constants.
 */
class constants {
public:
#if 0
    private static final Logger logger = LoggerFactory.getLogger(Constants.class);
#endif
public:
    enum class type {
        STRING, INTEGER, UUID, FLOAT, BOOLEAN, HEX, DURATION
    };

    /**
    * A constant value, i.e. a ByteBuffer.
    */
    class value : public terminal {
    public:
        cql3::raw_value _bytes;
        data_type _value_type;

        value(cql3::raw_value bytes_, data_type value_type_) : _bytes(std::move(bytes_)), _value_type(value_type_) {
            if (_value_type.get() == nullptr) {
                std::cout << fmt::format("TYPE IS NULLPTR: {}:{}", __FILE__, __LINE__) << std::endl;
                throw std::runtime_error(fmt::format("TYPE IS NULLPTR: {}:{}", __FILE__, __LINE__));
            }
        }
        virtual sstring to_string() const override { return _bytes.to_view().with_value([] (const FragmentedView auto& v) { return to_hex(v); }); }

        virtual rewrite::term to_new_term() const override {
            if (_bytes.is_null()) {
                return rewrite::term(cql_value(null_value{}));
            }

            if (_bytes.is_unset_value()) {
                return rewrite::term(cql_value(unset_value{}));
            }

            return rewrite::term(cql_value(serialized_value(cql3::raw_value(_bytes).to_bytes(), _value_type)));
        };
    };

    static thread_local const ::shared_ptr<value> UNSET_VALUE;

    class null_literal final : public term::raw {
    private:
        class null_value final : public value {
        public:
            null_value() : value(cql3::raw_value::make_null(), empty_type) {}
            virtual ::shared_ptr<terminal> bind(const query_options& options) override { return {}; }
            virtual sstring to_string() const override { return "null"; }

            virtual rewrite::term to_new_term() const override {
                return rewrite::term(cql_value(cql3::null_value{}));
            };
        };
    public:
        static thread_local const ::shared_ptr<terminal> NULL_VALUE;
        virtual ::shared_ptr<term> prepare(database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) const override {
            if (!is_assignable(test_assignment(db, keyspace, *receiver))) {
                throw exceptions::invalid_request_exception("Invalid null value for counter increment/decrement");
            }
            return NULL_VALUE;
        }

        virtual assignment_testable::test_result test_assignment(database& db,
            const sstring& keyspace,
            const column_specification& receiver) const override {
                return receiver.type->is_counter()
                    ? assignment_testable::test_result::NOT_ASSIGNABLE
                    : assignment_testable::test_result::WEAKLY_ASSIGNABLE;
        }

        virtual sstring to_string() const override {
            return "null";
        }
    };

    static thread_local const ::shared_ptr<term::raw> NULL_LITERAL;

    class literal : public term::raw {
    private:
        const type _type;
        const sstring _text;
    public:
        literal(type type_, sstring text)
            : _type{type_}
            , _text{text}
        {}

        static ::shared_ptr<literal> string(sstring text) {
            // This is a workaround for antlr3 not distinguishing between
            // calling in lexer setText() with an empty string and not calling
            // setText() at all.
            if (text.size() == 1 && text[0] == '\xFF') {
                text = {};
            }
            return ::make_shared<literal>(type::STRING, text);
        }

        static ::shared_ptr<literal> integer(sstring text) {
            return ::make_shared<literal>(type::INTEGER, text);
        }

        static ::shared_ptr<literal> floating_point(sstring text) {
            return ::make_shared<literal>(type::FLOAT, text);
        }

        static ::shared_ptr<literal> uuid(sstring text) {
            return ::make_shared<literal>(type::UUID, text);
        }

        static ::shared_ptr<literal> bool_(sstring text) {
            return ::make_shared<literal>(type::BOOLEAN, text);
        }

        static ::shared_ptr<literal> hex(sstring text) {
            return ::make_shared<literal>(type::HEX, text);
        }

        static ::shared_ptr<literal> duration(sstring text) {
            return ::make_shared<literal>(type::DURATION, text);
        }

        virtual ::shared_ptr<term> prepare(database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) const override;
    private:
        bytes parsed_value(data_type validator) const;
    public:
        const sstring& get_raw_text() {
            return _text;
        }

        virtual assignment_testable::test_result test_assignment(database& db, const sstring& keyspace, const column_specification& receiver) const;

        virtual sstring to_string() const override {
            return _type == type::STRING ? sstring(format("'{}'", _text)) : _text;
        }
    };

    class marker : public abstract_marker {
    public:
        marker(int32_t bind_index, lw_shared_ptr<column_specification> receiver)
            : abstract_marker{bind_index, std::move(receiver)}
        {
            assert(!_receiver->type->is_collection() && !_receiver->type->is_user_type());
        }

        virtual cql3::raw_value_view bind_and_get(const query_options& options) override {
            try {
                auto value = options.get_value_at(_bind_index);
                if (value) {
                    value.validate(*_receiver->type, options.get_cql_serialization_format());
                }
                return value;
            } catch (const marshal_exception& e) {
                throw exceptions::invalid_request_exception(
                        format("Exception while binding column {:s}: {:s}", _receiver->name->to_cql_string(), e.what()));
            }
        }

        virtual ::shared_ptr<terminal> bind(const query_options& options) override {
            auto bytes = bind_and_get(options);
            if (bytes.is_null()) {
                return ::shared_ptr<terminal>{};
            }
            if (bytes.is_unset_value()) {
                return UNSET_VALUE;
            }
            return ::make_shared<constants::value>(cql3::raw_value::make_value(bytes), _receiver->type);
        }

        virtual rewrite::term to_new_term() const override {
            return rewrite::term(rewrite::delayed_cql_value(rewrite::bound_value{_bind_index, _receiver}));
        };
    };

    class setter : public operation {
    public:
        using operation::operation;

        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override {
            auto value = _t->bind_and_get(params._options);
            execute(m, prefix, params, column, std::move(value));
        }

        static void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const column_definition& column, cql3::raw_value_view value) {
            if (value.is_null()) {
                m.set_cell(prefix, column, params.make_dead_cell());
            } else if (value.is_value()) {
                m.set_cell(prefix, column, params.make_cell(*column.type, value));
            }
        }
    };

    struct adder final : operation {
        using operation::operation;

        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override {
            auto value = _t->bind_and_get(params._options);
            if (value.is_null()) {
                throw exceptions::invalid_request_exception("Invalid null value for counter increment");
            } else if (value.is_unset_value()) {
                return;
            }
            auto increment = value.deserialize<int64_t>(*long_type);
            m.set_cell(prefix, column, params.make_counter_update_cell(increment));
        }
    };

    struct subtracter final : operation {
        using operation::operation;

        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override {
            auto value = _t->bind_and_get(params._options);
            if (value.is_null()) {
                throw exceptions::invalid_request_exception("Invalid null value for counter increment");
            } else if (value.is_unset_value()) {
                return;
            }
            auto increment = value.deserialize<int64_t>(*long_type);
            if (increment == std::numeric_limits<int64_t>::min()) {
                throw exceptions::invalid_request_exception(format("The negation of {:d} overflows supported counter precision (signed 8 bytes integer)", increment));
            }
            m.set_cell(prefix, column, params.make_counter_update_cell(-increment));
        }
    };

    class deleter : public operation {
    public:
        deleter(const column_definition& column)
            : operation(column, {})
        { }

        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };
};

std::ostream& operator<<(std::ostream&out, constants::type t);

}
