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

#include "cql3/constants.hh"
#include "cql3/cql3_type.hh"

namespace cql3 {

thread_local const ::shared_ptr<constants::value> constants::UNSET_VALUE = ::make_shared<constants::value>(cql3::raw_value::make_unset_value(), empty_type);
thread_local const ::shared_ptr<term::raw> constants::NULL_LITERAL = ::make_shared<constants::null_literal>();

static cql_value constant_to_cql_value(const abstract_type& value_type, bytes_view value_as_bytes_view) {
    switch (value_type.get_kind()) {
        case abstract_type::kind::ascii:
            return cql_value(ascii_value(value_as_bytes_view));

        case abstract_type::kind::boolean:
            return cql_value(bool_value(value_as_bytes_view));

        case abstract_type::kind::byte:
            return cql_value(int8_value(value_as_bytes_view));

        case abstract_type::kind::bytes:
            return cql_value(blob_value(value_as_bytes_view));

        case abstract_type::kind::counter:
            return cql_value(counter_value(value_as_bytes_view));

        case abstract_type::kind::date:
            return cql_value(date_value(value_as_bytes_view));

        case abstract_type::kind::decimal:
            return cql_value(decimal_value(value_as_bytes_view));

        case abstract_type::kind::double_kind:
            return cql_value(double_value(value_as_bytes_view));

        case abstract_type::kind::duration:
            return cql_value(duration_value(value_as_bytes_view));

        case abstract_type::kind::float_kind:
            return cql_value(float_value(value_as_bytes_view));

        case abstract_type::kind::inet:
            return cql_value(inet_value(value_as_bytes_view));

        case abstract_type::kind::int32:
            return cql_value(int32_value(value_as_bytes_view));

        case abstract_type::kind::long_kind:
            return cql_value(int64_value(value_as_bytes_view));

        case abstract_type::kind::short_kind:
            return cql_value(int16_value(value_as_bytes_view));

        case abstract_type::kind::simple_date:
            return cql_value(simple_date_value(value_as_bytes_view));

        case abstract_type::kind::time:
            return cql_value(time_value(value_as_bytes_view));

        case abstract_type::kind::timestamp:
            return cql_value(timestamp_value(value_as_bytes_view));

        case abstract_type::kind::timeuuid:
            return cql_value(timeuuid_value(value_as_bytes_view));

        case abstract_type::kind::utf8:
            return cql_value(utf8_value(value_as_bytes_view));

        case abstract_type::kind::uuid:
            return cql_value(uuid_value(value_as_bytes_view));

        case abstract_type::kind::varint:
            return cql_value(varint_value(value_as_bytes_view));

        case abstract_type::kind::list:
            throw std::runtime_error("constants::value can't be a list");

        case abstract_type::kind::map:
            throw std::runtime_error("constants::value can't be a map");

        case abstract_type::kind::set:
            throw std::runtime_error("constants::value can't be a set");

        case abstract_type::kind::tuple:
            throw std::runtime_error("constants::value can't be a tuple");

        case abstract_type::kind::user:
            throw std::runtime_error("constants::value can't be a user defined type");

        case abstract_type::kind::empty:
            throw std::runtime_error("can't convert empty constants::value to cql_value");
            
        case abstract_type::kind::reversed:
            throw std::runtime_error("constant_to_cql_value reversed type is not allowed");

        default:
            throw std::runtime_error("constants::value::to_cql_value - unhandled type");
    };
}

ordered_cql_value constants::value::to_ordered_cql_value() const {
    const bool should_reverse = _type->is_finally_reversed();

    if (_bytes.is_null()) {
        return reverse_if_needed(cql_value(null_value{}), should_reverse);
    }

    if (_bytes.is_unset_value()) {
        return reverse_if_needed(cql_value(unset_value{}), should_reverse);
    }

    bytes_view value_as_bytes_view = _bytes.to_view().with_linearized([] (bytes_view bv) {return bv;});
    const abstract_type& not_reversed_type = _type->final_without_reversed();
    cql_value cql_val = constant_to_cql_value(not_reversed_type, value_as_bytes_view);

    return reverse_if_needed(std::move(cql_val), should_reverse);
}

ordered_cql_value constants::null_literal::null_value::to_ordered_cql_value() const {
    if (_type->is_finally_reversed()) {
        return ordered_cql_value(cql_value(cql3::null_value{}));
    } else {
        return ordered_cql_value(reversed_cql_value{cql_value(cql3::null_value{})});
    }
}

std::ostream&
operator<<(std::ostream&out, constants::type t)
{
    switch (t) {
        case constants::type::STRING:   return out << "STRING";
        case constants::type::INTEGER:  return out << "INTEGER";
        case constants::type::UUID:     return out << "UUID";
        case constants::type::FLOAT:    return out << "FLOAT";
        case constants::type::BOOLEAN:  return out << "BOOLEAN";
        case constants::type::HEX:      return out << "HEX";
        case constants::type::DURATION: return out << "DURATION";
    }
    abort();
}

bytes
constants::literal::parsed_value(data_type validator) const
{
    try {
        if (_type == type::HEX && validator == bytes_type) {
            auto v = static_cast<sstring_view>(_text);
            v.remove_prefix(2);
            return validator->from_string(v);
        }
        if (validator->is_counter()) {
            return long_type->from_string(_text);
        }
        return validator->from_string(_text);
    } catch (const marshal_exception& e) {
        throw exceptions::invalid_request_exception(e.what());
    }
}

assignment_testable::test_result
constants::literal::test_assignment(database& db, const sstring& keyspace, const column_specification& receiver) const
{
    auto receiver_type = receiver.type->as_cql3_type();
    if (receiver_type.is_collection() || receiver_type.is_user_type()) {
        return test_result::NOT_ASSIGNABLE;
    }
    if (!receiver_type.is_native()) {
        return test_result::WEAKLY_ASSIGNABLE;
    }
    auto kind = receiver_type.get_kind();
    switch (_type) {
        case type::STRING:
            if (cql3_type::kind_enum_set::frozen<
                    cql3_type::kind::ASCII,
                    cql3_type::kind::TEXT,
                    cql3_type::kind::INET,
                    cql3_type::kind::TIMESTAMP,
                    cql3_type::kind::DATE,
                    cql3_type::kind::TIME>::contains(kind)) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case type::INTEGER:
            if (cql3_type::kind_enum_set::frozen<
                    cql3_type::kind::BIGINT,
                    cql3_type::kind::COUNTER,
                    cql3_type::kind::DECIMAL,
                    cql3_type::kind::DOUBLE,
                    cql3_type::kind::FLOAT,
                    cql3_type::kind::INT,
                    cql3_type::kind::SMALLINT,
                    cql3_type::kind::TIMESTAMP,
                    cql3_type::kind::DATE,
                    cql3_type::kind::TINYINT,
                    cql3_type::kind::VARINT>::contains(kind)) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case type::UUID:
            if (cql3_type::kind_enum_set::frozen<
                    cql3_type::kind::UUID,
                    cql3_type::kind::TIMEUUID>::contains(kind)) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case type::FLOAT:
            if (cql3_type::kind_enum_set::frozen<
                    cql3_type::kind::DECIMAL,
                    cql3_type::kind::DOUBLE,
                    cql3_type::kind::FLOAT>::contains(kind)) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case type::BOOLEAN:
            if (kind == cql3_type::kind_enum_set::prepare<cql3_type::kind::BOOLEAN>()) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case type::HEX:
            if (kind == cql3_type::kind_enum_set::prepare<cql3_type::kind::BLOB>()) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case type::DURATION:
            if (kind == cql3_type::kind_enum_set::prepare<cql3_type::kind::DURATION>()) {
                return assignment_testable::test_result::EXACT_MATCH;
            }
            break;
    }
    return assignment_testable::test_result::NOT_ASSIGNABLE;
}

::shared_ptr<term>
constants::literal::prepare(database& db, const sstring& keyspace, lw_shared_ptr<column_specification> receiver) const
{
    if (!is_assignable(test_assignment(db, keyspace, *receiver))) {
        throw exceptions::invalid_request_exception(format("Invalid {} constant ({}) for \"{}\" of type {}",
            _type, _text, *receiver->name, receiver->type->as_cql3_type().to_string()));
    }
    return ::make_shared<value>(cql3::raw_value::make_value(parsed_value(receiver->type)), receiver->type);
}

void constants::deleter::execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) {
    if (column.type->is_multi_cell()) {
        collection_mutation_description coll_m;
        coll_m.tomb = params.make_tombstone();

        m.set_cell(prefix, column, coll_m.serialize(*column.type));
    } else {
        m.set_cell(prefix, column, params.make_dead_cell());
    }
}

}
