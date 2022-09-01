#include "plan.hh"
#include "cartesian_product.hh"
#include "cql3/cql_config.hh"
#include "cql3/query_options.hh"
#include "types/list.hh"

namespace cql3 {
using namespace expr;

namespace plan {
struct split_where_clause {
    expression partition_key_restrictions;
    expression clustering_key_restrictions;
    expression other_restrictions;
};

bool contains_column(const expression& e) {
    const column_value* column = find_in_expression<column_value>(e, [](const column_value& col) { return true; });
    return column != nullptr;
}

bool contains_partition_key_column(const expression& e) {
    const column_value* partition_key_column =
        find_in_expression<column_value>(e, [](const column_value& col) { return col.col->is_partition_key(); });
    return partition_key_column != nullptr;
}

bool contains_clustering_key_column(const expression& e) {
    const column_value* clustering_key_column =
        find_in_expression<column_value>(e, [](const column_value& col) { return col.col->is_partition_key(); });
    return clustering_key_column != nullptr;
}

bool contains_static_column(const expression& e) {
    const column_value* static_column =
        find_in_expression<column_value>(e, [](const column_value& col) { return col.col->is_static(); });
    return static_column != nullptr;
}

bool contains_regular_column(const expression& e) {
    const column_value* regular_column =
        find_in_expression<column_value>(e, [](const column_value& col) { return col.col->is_regular(); });
    return regular_column != nullptr;
}

split_where_clause get_split_where_clause(const std::optional<expr::expression>& where_clause) {
    std::vector<expression> partition_key_restrictions;
    std::vector<expression> clustering_key_restrictions;
    std::vector<expression> other_restrictions;

    if (where_clause.has_value()) {
        recurse_until(*where_clause, [&](const expression& e) -> bool {
            bool has_partition = contains_partition_key_column(e);
            bool has_clustering = contains_clustering_key_column(e);
            bool has_regular = contains_regular_column(e);
            bool has_static = contains_static_column(e);
            bool has_static_or_regular = has_regular || has_static;

            if (has_partition && !has_clustering && !has_static_or_regular) {
                partition_key_restrictions.push_back(e);
            } else if (has_clustering && !has_partition && !has_static_or_regular) {
                clustering_key_restrictions.push_back(e);
                // Maybe it would be possible to allow static columns here.
                // Once we get a partition and clustering slice to visit,
                // we can decide whether to go through all rows or not
                // based on a condition which uses a static column.
                // And partition key!
            } else {
                other_restrictions.push_back(e);
            }

            return false;
        });
    }

    return split_where_clause{
        .partition_key_restrictions = conjunction{std::move(partition_key_restrictions)},
        .clustering_key_restrictions = conjunction{std::move(clustering_key_restrictions)},
        .other_restrictions = conjunction{std::move(other_restrictions)},
    };
}

std::pair<std::vector<binary_operator>, std::vector<expression>> extract_binops(const expression& e) {
    std::vector<binary_operator> binops;
    std::vector<expression> others;

    recurse_until(e, [&](const expression& e) -> bool {
        if (auto binop = as_if<binary_operator>(&e)) {
            binops.push_back(*binop);
        } else {
            others.push_back(e);
        }

        return false;
    });

    return {std::move(binops), std::move(others)};
}

range_bound range_from_binop(const binary_operator& binop) {
    assert(is_slice(binop.op));

    if (binop.op == oper_t::LT || binop.op == oper_t::LTE) {
        return range_bound{.type = range_bound::type::upper,
                           .value = binop.rhs,
                           .inclusive = (binop.op == oper_t::LTE),
                           .order = binop.order};
    } else if (binop.op == oper_t::GT || binop.op == oper_t::GTE) {
        return range_bound{.type = range_bound::type::lower,
                           .value = binop.rhs,
                           .inclusive = (binop.op == oper_t::GTE),
                           .order = binop.order};
    } else {
        throw std::runtime_error("Oh no");
    }
}

std::optional<std::pair<value_list_conditions, std::vector<binary_operator>>> try_extract_value_list(
    const std::vector<binary_operator>& binops) {
    std::optional<in_values> in_condition;
    std::vector<std::variant<in_values, range_bound>> other_conditions;
    std::vector<binary_operator> others;

    for (const binary_operator& binop : binops) {
        if (contains_column(binop.rhs)) {
            others.push_back(binop);
            continue;
        }

        if (binop.op == oper_t::EQ || binop.op == oper_t::IN) {
            in_values in_vals;

            if (binop.op == oper_t::EQ) {
                in_vals = in_values{.in_values_list =
                                        collection_constructor{.style = collection_constructor::style_type::list,
                                                               .elements = std::vector{binop.rhs}}};
            } else {
                in_vals = in_values{.in_values_list = binop.rhs};
            }

            if (!in_condition.has_value()) {
                in_condition = std::move(in_vals);
            } else {
                other_conditions.push_back(std::move(in_vals));
            }
        } else if (is_slice(binop.op)) {
            other_conditions.push_back(range_from_binop(binop));
        } else {
            others.push_back(binop);
        }
    }

    if (!in_condition.has_value()) {
        return std::nullopt;
    }

    value_list_conditions result{
        .in_list = std::move(*in_condition),
        .other_conditions = std::move(other_conditions),
    };

    return std::pair(std::move(result), std::move(others));
}

std::optional<std::pair<value_list_conditions, std::vector<expression>>> try_extract_value_list(const expression& e) {
    auto [binops, others] = extract_binops(e);

    std::optional<std::pair<value_list_conditions, std::vector<binary_operator>>> val_list_opt =
        try_extract_value_list(binops);
    if (!val_list_opt.has_value()) {
        return std::nullopt;
    }

    for (binary_operator& non_val_list_binop : val_list_opt->second) {
        others.push_back(std::move(non_val_list_binop));
    }

    return std::pair(std::move(val_list_opt->first), std::move(others));
}

std::optional<std::pair<range_conditions, std::vector<binary_operator>>> try_extract_range_conditions(
    const std::vector<binary_operator>& binops) {
    std::vector<range_bound> ranges;
    std::vector<binary_operator> others;

    for (const binary_operator& binop : binops) {
        if (is_slice(binop.op) && !contains_column(binop.rhs)) {
            ranges.push_back(range_from_binop(binop));
        } else {
            others.push_back(binop);
        }
    }

    if (ranges.empty()) {
        return std::nullopt;
    }

    range_conditions range_condts{.ranges = std::move(ranges)};

    return std::pair(std::move(range_condts), std::move(others));
}

std::optional<std::pair<range_conditions, std::vector<expression>>> try_extract_range_conditions(const expression& e) {
    auto [binops, others] = extract_binops(e);

    std::optional<std::pair<range_conditions, std::vector<binary_operator>>> ranges_opt =
        try_extract_range_conditions(binops);

    if (!ranges_opt.has_value()) {
        return std::nullopt;
    }

    for (binary_operator& non_val_list_binop : ranges_opt->second) {
        others.push_back(std::move(non_val_list_binop));
    }

    return std::pair(std::move(ranges_opt->first), std::move(others));
}

using column_binops_map =
    std::map<const column_definition*, std::vector<binary_operator>, schema_pos_column_definition_comparator>;

std::pair<column_binops_map, std::vector<binary_operator>> extract_column_binops(
    const std::vector<binary_operator>& binops) {
    column_binops_map result;
    std::vector<binary_operator> others;

    for (const binary_operator& binop : binops) {
        if (auto lhs_col = as_if<column_value>(&binop.lhs)) {
            result[lhs_col->col].push_back(binop);
        } else {
            others.push_back(binop);
        }
    }

    return {std::move(result), std::move(others)};
}

std::pair<column_binops_map, std::vector<expression>> extract_column_binops(const expression& e) {
    auto [binops, others] = extract_binops(e);

    auto [col_binops_map, other_binops] = extract_column_binops(binops);

    for (binary_operator& binop : other_binops) {
        others.push_back(std::move(binop));
    }
    return {std::move(col_binops_map), std::move(others)};
}

using column_value_lists_map =
    std::map<const column_definition*, value_list_conditions, schema_pos_column_definition_comparator>;

std::pair<column_value_lists_map, std::vector<binary_operator>> extract_column_value_lists(
    const column_binops_map& column_binops) {
    column_value_lists_map result;
    std::vector<binary_operator> others;
    for (auto& [col, col_binops] : column_binops) {
        std::optional<std::pair<value_list_conditions, std::vector<binary_operator>>> val_list_opt =
            try_extract_value_list(col_binops);

        if (val_list_opt.has_value()) {
            result[col] = std::move(val_list_opt->first);
            for (binary_operator& binop : val_list_opt->second) {
                others.push_back(std::move(binop));
            }
        } else {
            for (const binary_operator& binop : col_binops) {
                others.push_back(binop);
            }
        }
    }

    return {std::move(result), std::move(others)};
}

std::pair<column_value_lists_map, std::vector<expression>> extract_column_value_lists(const expression& e) {
    auto [col_binops, others] = extract_column_binops(e);
    auto [col_val_lists, other_binops] = extract_column_value_lists(col_binops);

    for (binary_operator& binop : other_binops) {
        others.push_back(std::move(binop));
    }

    return {std::move(col_val_lists), std::move(others)};
}

std::optional<std::pair<column_value_lists, std::optional<filter>>> try_get_partition_key_values(
    const expression& partition_key_restrictions,
    const schema& table_schema) {
    auto [col_value_lists, others] = extract_column_value_lists(partition_key_restrictions);

    // All partition key columns must be restricted to a list of values
    if (col_value_lists.size() != table_schema.partition_key_size()) {
        return std::nullopt;
    }

    column_value_lists col_vals_condition{.column_values = std::move(col_value_lists)};

    std::optional<filter> partition_filter;
    if (!others.empty()) {
        partition_filter = filter{.filter_expr = conjunction{std::move(others)}};
    }

    return std::pair(std::move(col_vals_condition), std::move(partition_filter));
}

using some_token_condition = std::variant<token_value_list, token_range>;

std::optional<std::pair<some_token_condition, std::optional<filter>>> try_get_token_condition(
    const expression& partition_key_restrictions) {
    std::vector<binary_operator> token_binops;
    std::vector<expression> others;

    recurse_until(partition_key_restrictions, [&](const expression& e) -> bool {
        if (auto binop = as_if<binary_operator>(&e)) {
            if (is<token>(binop->lhs)) {
                token_binops.push_back(*binop);
                return false;
            }
        }

        others.push_back(e);
        return false;
    });

    std::optional<std::pair<value_list_conditions, std::vector<binary_operator>>> token_vals_opt =
        try_extract_value_list(token_binops);
    if (token_vals_opt.has_value()) {
        token_value_list token_condition{
            .values = std::move(token_vals_opt->first),
        };

        for (binary_operator& binop : token_vals_opt->second) {
            others.push_back(std::move(binop));
        }
        std::optional<filter> others_filter;
        if (!others.empty()) {
            others_filter = filter{.filter_expr = conjunction{std::move(others)}};
        }
        return std::pair(std::move(token_condition), std::move(others_filter));
    }

    std::optional<std::pair<range_conditions, std::vector<binary_operator>>> token_ranges_opt =
        try_extract_range_conditions(token_binops);
    if (!token_ranges_opt.has_value()) {
        return std::nullopt;
    }

    token_range token_ranges{.ranges = std::move(token_ranges_opt->first)};

    for (binary_operator& binop : token_ranges_opt->second) {
        others.push_back(std::move(binop));
    }
    std::optional<filter> others_filter;
    if (!others.empty()) {
        others_filter = filter{.filter_expr = conjunction{std::move(others)}};
    }

    return std::pair(std::move(token_ranges), std::move(others_filter));
}

std::pair<partition_key_condition, std::optional<filter>> partition_condition_from_restrictions(
    const expression& partition_key_restrictions,
    const schema& table_schema) {
    if (is_empty_restriction(partition_key_restrictions)) {
        return {no_condition{}, std::nullopt};
    }

    // Try to find a list of values for each partition key column
    std::optional<std::pair<column_value_lists, std::optional<filter>>> col_val_lists_opt =
        try_get_partition_key_values(partition_key_restrictions, table_schema);
    if (col_val_lists_opt.has_value()) {
        return {std::move(col_val_lists_opt->first), std::move(col_val_lists_opt->second)};
    }

    // Try to find a restriction on token
    std::optional<std::pair<some_token_condition, std::optional<filter>>> token_cond_opt =
        try_get_token_condition(partition_key_restrictions);
    if (token_cond_opt.has_value()) {
        return std::visit(
            overloaded_functor{
                [&](token_value_list& token_vals) -> std::pair<partition_key_condition, std::optional<filter>> {
                    return {std::move(token_vals), std::move(token_cond_opt->second)};
                },
                [&](token_range& token_ranges) -> std::pair<partition_key_condition, std::optional<filter>> {
                    return {std::move(token_ranges), std::move(token_cond_opt->second)};
                }},
            token_cond_opt->first);
    }

    // Otherwise filter everything
    filter all_filter{.filter_expr = partition_key_restrictions};
    return {no_condition{}, std::move(all_filter)};
}

using single_col_clustering_condtion = std::variant<column_value_lists, single_column_clustering_range>;

std::optional<std::pair<single_col_clustering_condtion, std::optional<filter>>> try_get_clustering_single_column(
    const expression& clustering_key_restrictions,
    const schema& table_schema) {
    auto [col_binops, others] = extract_column_binops(clustering_key_restrictions);

    column_value_lists_map prefix_col_values;
    const column_definition* last_prefix_column = nullptr;
    range_conditions last_prefix_column_ranges;

    for (const column_definition& cur_col : table_schema.clustering_key_columns()) {
        auto cur_col_binops = col_binops.find(&cur_col);
        if (cur_col_binops == col_binops.end()) {
            break;
        }

        std::optional<std::pair<value_list_conditions, std::vector<binary_operator>>> cur_col_vals =
            try_extract_value_list(cur_col_binops->second);

        if (cur_col_vals.has_value()) {
            prefix_col_values[&cur_col] = std::move(cur_col_vals->first);
            for (binary_operator& binop : cur_col_vals->second) {
                others.push_back(std::move(binop));
            }
            continue;
        }

        std::optional<std::pair<range_conditions, std::vector<binary_operator>>> cur_col_ranges =
            try_extract_range_conditions(cur_col_binops->second);
        if (!cur_col_vals.has_value()) {
            break;
        }

        last_prefix_column = &cur_col;
        last_prefix_column_ranges = std::move(cur_col_ranges->first);
        for (binary_operator& binop : cur_col_ranges->second) {
            others.push_back(std::move(binop));
        }
        break;
    }

    for (auto& [cdef, cdef_binops] : col_binops) {
        if (prefix_col_values.contains(cdef) || last_prefix_column == cdef) {
            continue;
        }

        for (binary_operator& binop : cdef_binops) {
            others.push_back(std::move(binop));
        }
    }

    if (prefix_col_values.empty() && last_prefix_column == nullptr) {
        return std::nullopt;
    }

    std::optional<filter> others_filter;
    if (!others.empty()) {
        others_filter = filter{.filter_expr = conjunction{std::move(others)}};
    }

    column_value_lists clustering_prefix_col_value_lists{.column_values = std::move(prefix_col_values)};

    if (last_prefix_column == nullptr) {
        return std::pair(std::move(clustering_prefix_col_value_lists), std::move(others_filter));
    }

    single_column_clustering_range prefix_range{.prefix_column_values = std::move(clustering_prefix_col_value_lists),
                                                .last_prefix_column = last_prefix_column,
                                                .last_column_range = std::move(last_prefix_column_ranges)};

    return std::pair(std::move(prefix_range), std::move(others_filter));
}

std::pair<std::vector<binary_operator>, std::vector<expression>> extract_multi_column_binops(const expression& e) {
    std::vector<binary_operator> multi_col_binops;
    std::vector<expression> others;

    recurse_until(e, [&](const expression& cur_expr) -> bool {
        if (auto binop = as_if<binary_operator>(&cur_expr)) {
            if (is_multi_column(*binop)) {
                multi_col_binops.push_back(*binop);
                return false;
            }
        }

        others.push_back(cur_expr);
        return false;
    });

    return {std::move(multi_col_binops), std::move(others)};
}

bool is_multi_col_clustering_prefix_restriction(const binary_operator& binop) {
    uint32_t next_expected_col_position = 0;
    bool all_not_reversed = true;
    bool all_reversed = true;

    const tuple_constructor* lhs_tuple = as_if<tuple_constructor>(&binop.lhs);
    if (lhs_tuple == nullptr) {
        return false;
    }

    if (lhs_tuple->elements.empty()) {
        return false;
    }

    for (const expression& lhs_tuple_elem : lhs_tuple->elements) {
        const column_value* col_val = as_if<column_value>(&lhs_tuple_elem);
        if (col_val == nullptr) {
            return false;
        }
        if (!col_val->col->is_clustering_key()) {
            return false;
        }
        if (col_val->col->position() != next_expected_col_position) {
            return false;
        }

        if (col_val->col->type->is_reversed()) {
            all_not_reversed = false;
        } else {
            all_reversed = false;
        }

        next_expected_col_position += 1;
    }

    bool is_mixed_ordering = (!all_not_reversed && !all_reversed);
    if (is_mixed_ordering && binop.order != comparison_order::clustering) {
        return false;
    }

    return true;
}

bool is_multi_col_clustering_prefix_restriction(const expression& e) {
    if (auto binop = as_if<binary_operator>(&e)) {
        return is_multi_col_clustering_prefix_restriction(*binop);
    }

    return false;
}

std::vector<const column_definition*> get_multi_col_columns(const binary_operator& binop) {
    // TODO: nullptr checks
    std::vector<const column_definition*> result;
    const tuple_constructor* lhs_tuple = as_if<tuple_constructor>(&binop.lhs);
    for (const expression& lhs_tuple_elem : lhs_tuple->elements) {
        const column_value* lhs_tuple_col = as_if<column_value>(&lhs_tuple_elem);
        result.push_back(lhs_tuple_col->col);
    }

    return result;
}

std::optional<std::pair<multi_column_in_values, std::optional<filter>>> try_get_multi_column_in(
    const expression& restrictions) {
    auto [binops, others] = extract_binops(restrictions);

    const binary_operator* best_binop = nullptr;
    size_t best_binop_lhs_len = 0;

    for (binary_operator& binop : binops) {
        if (!is_multi_col_clustering_prefix_restriction(binop)) {
            others.push_back(binop);
            continue;
        }

        if (binop.op != oper_t::EQ && binop.op != oper_t::IN) {
            others.push_back(binop);
            continue;
        }

        if (contains_column(binop.rhs)) {
            others.push_back(binop);
            continue;
        }

        const tuple_constructor* lhs_tup = as_if<tuple_constructor>(&binop.lhs);
        if (lhs_tup->elements.size() > best_binop_lhs_len) {
            if (best_binop != nullptr) {
                others.push_back(*best_binop);
                best_binop = &binop;
                best_binop_lhs_len = lhs_tup->elements.size();
                continue;
            }
        }

        others.push_back(binop);
    }

    if (best_binop == nullptr) {
        return std::nullopt;
    }

    std::vector<const column_definition*> lhs_cols = get_multi_col_columns(*best_binop);

    expression col_in_values;
    if (best_binop->op == oper_t::EQ) {
        col_in_values =
            collection_constructor{.style = collection_constructor::style_type::list, .elements = {best_binop->rhs}};
    } else if (best_binop->op == oper_t::IN) {
        col_in_values = best_binop->rhs;
    } else {
        // TODO: throw or smth
    }

    multi_column_in_values result{.columns = std::move(lhs_cols), .in_values = std::move(col_in_values)};

    std::optional<filter> others_filter;
    if (!others.empty()) {
        others_filter = filter{.filter_expr = conjunction{std::move(others)}};
    }

    return std::pair(std::move(result), std::move(others_filter));
}

std::optional<std::pair<multi_column_clustering_range, std::optional<filter>>> try_get_multi_column_range(
    const expression& restrictions) {
    std::vector<binary_operator> range_binops;
    std::vector<expression> others;

    recurse_until(restrictions, [&](const expression& e) -> bool {
        if (auto binop = as_if<binary_operator>(&e)) {
            if (is_multi_col_clustering_prefix_restriction(*binop) && is_slice(binop->op) &&
                !contains_column(binop->rhs)) {
                range_binops.push_back(*binop);
                return false;
            }
        }

        others.push_back(e);
        return false;
    });

    if (range_binops.empty()) {
        return std::nullopt;
    }

    std::vector<const column_definition*> clustering_prefix;
    std::vector<range_bound> ranges;

    for (const binary_operator& range_binop : range_binops) {
        std::vector<const column_definition*> cur_columns = get_multi_col_columns(range_binop);
        if (cur_columns.size() > clustering_prefix.size()) {
            clustering_prefix = std::move(cur_columns);
        }
        range_bound cur_range = range_from_binop(range_binop);
        ranges.push_back(std::move(cur_range));
    }

    multi_column_clustering_range result{.clustering_prefix = std::move(clustering_prefix),
                                         .prefix_range = range_conditions{.ranges = std::move(ranges)}};

    std::optional<filter> others_filter;
    if (!others.empty()) {
        others_filter = filter{.filter_expr = conjunction{std::move(others)}};
    }

    return std::pair(std::move(result), std::move(others_filter));
}

std::pair<clustering_key_condition, std::optional<filter>> clustering_condition_from_restrictions(
    const expression& clustering_key_restrictions,
    const schema& table_schema) {
    if (is_empty_restriction(clustering_key_restrictions)) {
        return {no_condition{}, std::nullopt};
    }

    // It would be possible to generate clustering ranges from all three of these
    // types of restrictions For now we take the first one available.
    std::optional<std::pair<single_col_clustering_condtion, std::optional<filter>>> single_col_opt =
        try_get_clustering_single_column(clustering_key_restrictions, table_schema);

    if (single_col_opt.has_value()) {
        return std::visit(
            overloaded_functor{
                [&](column_value_lists& col_val_lists) -> std::pair<clustering_key_condition, std::optional<filter>> {
                    return std::pair(std::move(col_val_lists), std::move(single_col_opt->second));
                },
                [&](single_column_clustering_range& single_col_range)
                    -> std::pair<clustering_key_condition, std::optional<filter>> {
                    return std::pair(std::move(single_col_range), std::move(single_col_opt->second));
                }},
            single_col_opt->first);
    }

    std::optional<std::pair<multi_column_in_values, std::optional<filter>>> multi_col_in_opt =
        try_get_multi_column_in(clustering_key_restrictions);

    if (multi_col_in_opt.has_value()) {
        return std::pair(std::move(multi_col_in_opt->first), std::move(multi_col_in_opt->second));
    }

    std::optional<std::pair<multi_column_clustering_range, std::optional<filter>>> multi_col_range_opt =
        try_get_multi_column_range(clustering_key_restrictions);

    if (multi_col_range_opt.has_value()) {
        return std::pair(std::move(multi_col_range_opt->first), std::move(multi_col_range_opt->second));
    }

    filter all_filter{.filter_expr = clustering_key_restrictions};
    return {no_condition{}, std::move(all_filter)};
}

single_table_query single_table_query_from_restrictions(const restrictions::statement_restrictions& restrictions) {
    assert(!restrictions.uses_secondary_indexing());

    split_where_clause split_where = get_split_where_clause(restrictions.get_where_clause());

    schema_ptr table_schema = restrictions.get_schema();
    auto [partition_condition, partition_filter] =
        partition_condition_from_restrictions(split_where.partition_key_restrictions, *table_schema);

    auto [clustering_condition, clustering_filter] =
        clustering_condition_from_restrictions(split_where.clustering_key_restrictions, *table_schema);

    std::optional<filter> other_filter;
    if (!expr::is_empty_restriction(split_where.other_restrictions)) {
        other_filter = filter{.filter_expr = split_where.other_restrictions};
    }

    return single_table_query{
        .table_schema = std::move(table_schema),
        .where = restrictions.get_where_clause(),
        .partition_key_restrictions = split_where.partition_key_restrictions,
        .partition_key_restrictions_map = get_single_column_restrictions_map(split_where.partition_key_restrictions),
        .partition_condition = std::move(partition_condition),
        .partition_filter = std::move(partition_filter),
        .clustering_key_restrictions = split_where.clustering_key_restrictions,
        .clustering_key_restrictions_map = get_single_column_restrictions_map(split_where.clustering_key_restrictions),
        .clustering_condition = std::move(clustering_condition),
        .clustering_filter = std::move(clustering_filter),
        .nonprimary_key_restrictions = split_where.other_restrictions,
        .nonprimary_key_restrictions_map = get_single_column_restrictions_map(split_where.other_restrictions),
        .other_filter = std::move(other_filter)};
}

/// Turns a partition-key value into a partition_range. \p pk must have elements
/// for all partition columns.
dht::partition_range range_from_bytes(const schema& schema, const std::vector<managed_bytes>& pk) {
    const auto k = partition_key::from_exploded(pk);
    const auto tok = dht::get_token(schema, k);
    const query::ring_position pos(std::move(tok), std::move(k));
    return dht::partition_range::make_singular(std::move(pos));
}

static dht::partition_range_vector partition_key_ranges_from_column_values(const column_value_lists& column_values,
                                                                           const query_options& options,
                                                                           const schema& table_schema) {
    const size_t size_limit =
        options.get_cql_config().restrictions.partition_key_restrictions_max_cartesian_product_size;
    // Each element is a vector of that column's possible values:
    std::vector<std::vector<managed_bytes>> col_vals;
    size_t product_size = 1;
    for (const auto& [col_def, col_values] : column_values.column_values) {
        value_list cur_col_vals = col_values.get_value_set(options);
        if (cur_col_vals.empty()) {
            return {};
        }
        product_size *= cur_col_vals.size();
        if (product_size > size_limit) {
            throw std::runtime_error(fmt::format("partition-key cartesian product size {} is greater than maximum {}",
                                                 product_size, size_limit));
        }
        col_vals.push_back(std::move(cur_col_vals));
    }
    cartesian_product cp(col_vals);
    dht::partition_range_vector ranges(product_size);
    std::transform(cp.begin(), cp.end(), ranges.begin(), std::bind_front(range_from_bytes, std::ref(table_schema)));
    return ranges;
}

static dht::partition_range_vector partition_key_ranges_from_token_values(value_set token_vals) {
    return std::visit(
        overloaded_functor{
            [](const value_list& token_vals) -> dht::partition_range_vector {
                dht::partition_range_vector result;
                for (const managed_bytes& token_val : token_vals) {
                    dht::token cur_token_val =
                        token_val.with_linearized([](bytes_view bv) { return dht::token::from_bytes(bv); });
                    auto start = dht::partition_range::bound(dht::ring_position::starting_at(cur_token_val));
                    auto end = dht::partition_range::bound(dht::ring_position::ending_at(cur_token_val));
                    result.push_back({start, end});
                }
                return result;
            },
            [](const nonwrapping_range<managed_bytes>& bounds) -> dht::partition_range_vector {
                const auto start_token = bounds.start() ? bounds.start()->value().with_linearized(
                                                              [](bytes_view bv) { return dht::token::from_bytes(bv); })
                                                        : dht::minimum_token();
                auto end_token = bounds.end() ? bounds.end()->value().with_linearized(
                                                    [](bytes_view bv) { return dht::token::from_bytes(bv); })
                                              : dht::maximum_token();
                const bool include_start = bounds.start() && bounds.start()->is_inclusive();
                const auto include_end = bounds.end() && bounds.end()->is_inclusive();

                auto start = dht::partition_range::bound(include_start ? dht::ring_position::starting_at(start_token)
                                                                       : dht::ring_position::ending_at(start_token));
                auto end = dht::partition_range::bound(include_end ? dht::ring_position::ending_at(end_token)
                                                                   : dht::ring_position::starting_at(end_token));

                return {{std::move(start), std::move(end)}};
            }},
        token_vals);
}

dht::partition_range_vector single_table_query::get_partition_key_ranges(const query_options& options) const {
    return std::visit(overloaded_functor{[](const no_condition&) -> dht::partition_range_vector {
                                             return {dht::partition_range::make_open_ended_both_sides()};
                                         },
                                         [&](const column_value_lists& column_values) -> dht::partition_range_vector {
                                             return partition_key_ranges_from_column_values(column_values, options,
                                                                                            *table_schema);
                                         },
                                         [&](const token_value_list& token_values) -> dht::partition_range_vector {
                                             value_set token_vals = token_values.values.get_value_set(options);
                                             return partition_key_ranges_from_token_values(std::move(token_vals));
                                         },
                                         [&](const token_range& tok_range) -> dht::partition_range_vector {
                                             value_set token_vals = tok_range.ranges.get_value_set(options);
                                             return partition_key_ranges_from_token_values(std::move(token_vals));
                                         }},
                      partition_condition);
}

struct range_less {
    const class schema& s;
    clustering_key_prefix::less_compare cmp = clustering_key_prefix::less_compare(s);
    bool operator()(const query::clustering_range& x, const query::clustering_range& y) const {
        if (!x.start() && !y.start()) {
            return false;
        }
        if (!x.start()) {
            return true;
        }
        if (!y.start()) {
            return false;
        }
        return cmp(x.start()->value(), y.start()->value());
    }
};

static std::vector<query::clustering_range> clustering_bounds_from_column_values(
    const column_value_lists& column_values,
    const query_options& options,
    const schema& table_schema) {
    const size_t size_limit =
        options.get_cql_config().restrictions.clustering_key_restrictions_max_cartesian_product_size;
    size_t product_size = 1;
    // Each element is a vector of that column's possible values:
    std::vector<std::vector<managed_bytes>> col_vals;
    for (const auto& [col_def, col_values] : column_values.column_values) {
        value_list cur_col_vals = col_values.get_value_set(options);
        if (cur_col_vals.empty()) {
            return {};
        }
        product_size *= cur_col_vals.size();
        if (product_size > size_limit) {
            throw std::runtime_error(fmt::format("clustering-key cartesian product size {} is greater than maximum {}",
                                                 product_size, size_limit));
        }
        col_vals.push_back(std::move(cur_col_vals));
    }

    std::vector<query::clustering_range> ck_ranges(product_size);
    cartesian_product cp(col_vals);
    std::transform(cp.begin(), cp.end(), ck_ranges.begin(), std::bind_front(query::clustering_range::make_singular));
    sort(ck_ranges.begin(), ck_ranges.end(), range_less{table_schema});
    return ck_ranges;
}

/// Reverses the range if the type is reversed.  Why don't we have nonwrapping_interval::reverse()??
static query::clustering_range reverse_if_reqd(query::clustering_range r, const abstract_type& t) {
    return t.is_reversed() ? query::clustering_range(r.end(), r.start()) : std::move(r);
}

static std::vector<query::clustering_range> clustering_bounds_from_single_column_clustering_range(
    const single_column_clustering_range& column_values,
    const query_options& options,
    const schema& table_schema) {
    const size_t size_limit =
        options.get_cql_config().restrictions.clustering_key_restrictions_max_cartesian_product_size;
    size_t product_size = 1;
    // Each element is a vector of that column's possible values:
    std::vector<std::vector<managed_bytes>> prefix_col_vals;
    for (const auto& [col_def, col_values] : column_values.prefix_column_values.column_values) {
        value_list cur_col_vals = col_values.get_value_set(options);
        if (cur_col_vals.empty()) {
            return {};
        }
        product_size *= cur_col_vals.size();
        if (product_size > size_limit) {
            throw std::runtime_error(fmt::format("clustering-key cartesian product size {} is greater than maximum {}",
                                                 product_size, size_limit));
        }
        prefix_col_vals.push_back(std::move(cur_col_vals));
    }

    nonwrapping_range<managed_bytes> last_col_range = column_values.last_column_range.get_value_set(options);

    std::vector<query::clustering_range> ck_ranges;
    if (prefix_col_vals.empty()) {
        ck_ranges.push_back(reverse_if_reqd(last_col_range.transform([](const managed_bytes& val) {
            return clustering_key_prefix::from_range(std::array<managed_bytes, 1>{val});
        }),
                                            *column_values.last_prefix_column->type));
    }

    ck_ranges.reserve(product_size);
    const auto extra_lb = last_col_range.start(), extra_ub = last_col_range.end();
    for (const std::vector<managed_bytes>& b : cartesian_product(prefix_col_vals)) {
        auto new_lb = b, new_ub = b;
        if (extra_lb) {
            new_lb.push_back(extra_lb->value());
        }
        if (extra_ub) {
            new_ub.push_back(extra_ub->value());
        }
        query::clustering_range::bound new_start(new_lb, extra_lb ? extra_lb->is_inclusive() : true);
        query::clustering_range::bound new_end(new_ub, extra_ub ? extra_ub->is_inclusive() : true);
        ck_ranges.push_back(reverse_if_reqd({new_start, new_end}, *column_values.last_prefix_column->type));
    }

    sort(ck_ranges.begin(), ck_ranges.end(), range_less{table_schema});
    return ck_ranges;
}

std::vector<query::clustering_range> clustering_bounds_from_multi_column_in(const multi_column_in_values& multi_col_in,
                                                                            const query_options& options,
                                                                            const schema& table_schema) {
    std::vector<query::clustering_range> ck_ranges;

    cql3::raw_value in_values = evaluate(multi_col_in.in_values, options);
    utils::chunked_vector<managed_bytes> in_values_list = get_list_elements(in_values);
    ck_ranges.reserve(in_values_list.size());

    for (const managed_bytes& column_vals : in_values_list) {
        utils::chunked_vector<managed_bytes> single_column_vals =
            get_list_elements(cql3::raw_value::make_value(column_vals));
        std::vector<managed_bytes> values_vec;
        values_vec.reserve(single_column_vals.size());
        for (managed_bytes& column_val : single_column_vals) {
            values_vec.push_back(std::move(column_val));
        }

        ck_ranges.push_back(query::clustering_range::make_singular(std::move(values_vec)));
    }

    sort(ck_ranges.begin(), ck_ranges.end(), range_less{table_schema});
    return ck_ranges;
}

/// True iff r1 start is strictly before r2 start.
bool starts_before_start(const query::clustering_range& r1,
                         const query::clustering_range& r2,
                         const clustering_key_prefix::prefix_equal_tri_compare& cmp) {
    if (!r2.start()) {
        return false;  // r2 start is -inf, nothing is before that.
    }
    if (!r1.start()) {
        return true;  // r1 start is -inf, while r2 start is finite.
    }
    const auto diff = cmp(r1.start()->value(), r2.start()->value());
    if (diff < 0) {  // r1 start is strictly before r2 start.
        return true;
    }
    if (diff > 0) {  // r1 start is strictly after r2 start.
        return false;
    }
    const auto len1 = r1.start()->value().representation().size();
    const auto len2 = r2.start()->value().representation().size();
    if (len1 == len2) {  // The values truly are equal.
        return r1.start()->is_inclusive() && !r2.start()->is_inclusive();
    } else if (len1 < len2) {  // r1 start is a prefix of r2 start.
        // (a)>=(1) starts before (a,b)>=(1,1), but (a)>(1) doesn't.
        return r1.start()->is_inclusive();
    } else {  // r2 start is a prefix of r1 start.
        // (a,b)>=(1,1) starts before (a)>(1) but after (a)>=(1).
        return r2.start()->is_inclusive();
    }
}

/// True iff r1 start is before (or identical as) r2 end.
bool starts_before_or_at_end(const query::clustering_range& r1,
                             const query::clustering_range& r2,
                             const clustering_key_prefix::prefix_equal_tri_compare& cmp) {
    if (!r1.start()) {
        return true;  // r1 start is -inf, must be before r2 end.
    }
    if (!r2.end()) {
        return true;  // r2 end is +inf, everything is before it.
    }
    const auto diff = cmp(r1.start()->value(), r2.end()->value());
    if (diff < 0) {  // r1 start is strictly before r2 end.
        return true;
    }
    if (diff > 0) {  // r1 start is strictly after r2 end.
        return false;
    }
    const auto len1 = r1.start()->value().representation().size();
    const auto len2 = r2.end()->value().representation().size();
    if (len1 == len2) {  // The values truly are equal.
        return r1.start()->is_inclusive() && r2.end()->is_inclusive();
    } else if (len1 < len2) {  // r1 start is a prefix of r2 end.
        // a>=(1) starts before (a,b)<=(1,1) ends, but (a)>(1) doesn't.
        return r1.start()->is_inclusive();
    } else {  // r2 end is a prefix of r1 start.
        // (a,b)>=(1,1) starts before (a)<=(1) ends but after (a)<(1) ends.
        return r2.end()->is_inclusive();
    }
}

/// True if r1 end is strictly before r2 end.
bool ends_before_end(const query::clustering_range& r1,
                     const query::clustering_range& r2,
                     const clustering_key_prefix::prefix_equal_tri_compare& cmp) {
    if (!r1.end()) {
        return false;  // r1 end is +inf, which is after everything.
    }
    if (!r2.end()) {
        return true;  // r2 end is +inf, while r1 end is finite.
    }
    const auto diff = cmp(r1.end()->value(), r2.end()->value());
    if (diff < 0) {  // r1 end is strictly before r2 end.
        return true;
    }
    if (diff > 0) {  // r1 end is strictly after r2 end.
        return false;
    }
    const auto len1 = r1.end()->value().representation().size();
    const auto len2 = r2.end()->value().representation().size();
    if (len1 == len2) {  // The values truly are equal.
        return !r1.end()->is_inclusive() && r2.end()->is_inclusive();
    } else if (len1 < len2) {  // r1 end is a prefix of r2 end.
        // (a)<(1) ends before (a,b)<=(1,1), but (a)<=(1) doesn't.
        return !r1.end()->is_inclusive();
    } else {  // r2 end is a prefix of r1 end.
        // (a,b)<=(1,1) ends before (a)<=(1) but after (a)<(1).
        return r2.end()->is_inclusive();
    }
}

std::optional<query::clustering_range> intersection(const query::clustering_range& r1,
                                                    const query::clustering_range& r2,
                                                    const clustering_key_prefix::prefix_equal_tri_compare& cmp) {
    // Assume r1's start is to the left of r2's start.
    if (starts_before_start(r2, r1, cmp)) {
        return intersection(r2, r1, cmp);
    }
    if (!starts_before_or_at_end(r2, r1, cmp)) {
        return {};
    }
    const auto& intersection_start = r2.start();
    const auto& intersection_end = ends_before_end(r1, r2, cmp) ? r1.end() : r2.end();
    if (intersection_start == intersection_end && intersection_end.has_value()) {
        return query::clustering_range::make_singular(intersection_end->value());
    }
    return query::clustering_range(intersection_start, intersection_end);
}

clustering_key_prefix::prefix_equal_tri_compare get_unreversed_tri_compare(const schema& schema) {
    clustering_key_prefix::prefix_equal_tri_compare cmp(schema);
    std::vector<data_type> types = cmp.prefix_type->types();
    for (auto& t : types) {
        if (t->is_reversed()) {
            t = t->underlying_type();
        }
    }
    cmp.prefix_type = make_lw_shared<compound_type<allow_prefixes::yes>>(types);
    return cmp;
}

std::vector<query::clustering_range> clustering_bounds_from_multi_col_ranges(
    const multi_column_clustering_range& multi_col_range,
    const query_options& options,
    const schema& table_schema) {
    std::optional<query::clustering_range> result = query::clustering_range::make_open_ended_both_sides();

    clustering_key_prefix::prefix_equal_tri_compare tri_cmp = get_unreversed_tri_compare(table_schema);

    for (const range_bound& range : multi_col_range.prefix_range.ranges) {
        if (!range.value.has_value()) {
            continue;
        }
        cql3::raw_value range_value = evaluate(*range.value, options);
        utils::chunked_vector<managed_bytes> column_values = get_list_elements(range_value);
        std::vector<managed_bytes> col_vals;
        col_vals.reserve(column_values.size());
        for (managed_bytes& column_val : column_values) {
            col_vals.push_back(std::move(column_val));
        }
        clustering_key_prefix prefix_value = clustering_key_prefix::from_range(column_values);
        query::clustering_range::bound bound(prefix_value, range.inclusive);
        query::clustering_range cur_range = range.type == range_bound::type::lower
                                                ? query::clustering_range::make_starting_with(bound)
                                                : query::clustering_range::make_ending_with(bound);
        if (!result.has_value()) {
            return {};
        }
        result = intersection(*result, cur_range, tri_cmp);
    }

    if (!result.has_value()) {
        return {};
    }

    return {*result};
}

std::vector<query::clustering_range> clustering_bounds_from_multi_column_clustering_order_ranges(
    const multi_column_clustering_range& multi_col_range,
    const query_options& options,
    const schema& table_schema) {
    std::optional<query::clustering_range::bound> lb, ub;
    for (const range_bound& range : multi_col_range.prefix_range.ranges) {
        if (!range.value.has_value()) {
            continue;
        }
        cql3::raw_value range_value = evaluate(*range.value, options);
        utils::chunked_vector<managed_bytes> column_values = get_list_elements(range_value);
        std::vector<managed_bytes> col_vals;
        col_vals.reserve(column_values.size());
        for (managed_bytes& column_val : column_values) {
            col_vals.push_back(std::move(column_val));
        }
        clustering_key_prefix prefix_value = clustering_key_prefix::from_range(column_values);
        query::clustering_range::bound bound(prefix_value, range.inclusive);
        query::clustering_range cur_range = range.type == range_bound::type::lower
                                                ? query::clustering_range::make_starting_with(bound)
                                                : query::clustering_range::make_ending_with(bound);
        if (cur_range.start()) {
            if (lb.has_value()) {
                throw std::runtime_error("Two clustering order lbs!");
            }
            lb = cur_range.start();
        }
        if (cur_range.end()) {
            if (ub.has_value()) {
                throw std::runtime_error("Two clustering order ubs!");
            }
            ub = cur_range.end();
        }
    }
    return {{lb, ub}};
}

std::vector<query::clustering_range> single_table_query::get_clustering_bounds(const query_options& options) const {
    return std::visit(
        overloaded_functor{
            [](const no_condition&) -> std::vector<query::clustering_range> {
                return {query::clustering_range::make_open_ended_both_sides()};
            },
            [&](const column_value_lists& column_values) -> std::vector<query::clustering_range> {
                return clustering_bounds_from_column_values(column_values, options, *table_schema);
            },
            [&](const multi_column_in_values& multi_col_in) -> std::vector<query::clustering_range> {
                return clustering_bounds_from_multi_column_in(multi_col_in, options, *table_schema);
            },
            [&](const single_column_clustering_range& single_col_vals) -> std::vector<query::clustering_range> {
                return clustering_bounds_from_single_column_clustering_range(single_col_vals, options, *table_schema);
            },
            [&](const multi_column_clustering_range& multi_col_range) -> std::vector<query::clustering_range> {
                for (const range_bound& range : multi_col_range.prefix_range.ranges) {
                    if (range.order == expr::comparison_order::clustering) {
                        return clustering_bounds_from_multi_column_clustering_order_ranges(multi_col_range, options,
                                                                                           *table_schema);
                    }
                }

                bool all_natural = true, all_reverse = true;
                for (const column_definition* cdef : multi_col_range.clustering_prefix) {
                    if (cdef->type->is_reversed()) {
                        all_natural = false;
                    } else {
                        all_reverse = false;
                    }
                }
                std::vector<query::clustering_range> bounds =
                    clustering_bounds_from_multi_col_ranges(multi_col_range, options, *table_schema);
                if (!all_natural && !all_reverse) {
                    std::vector<query::clustering_range> bounds_in_clustering_order;
                    for (const auto& b : bounds) {
                        const auto eqv = restrictions::get_equivalent_ranges(b, *table_schema);
                        bounds_in_clustering_order.insert(bounds_in_clustering_order.end(), eqv.cbegin(), eqv.cend());
                    }
                    return bounds_in_clustering_order;
                }
                if (all_reverse) {
                    for (auto& crange : bounds) {
                        crange = query::clustering_range(crange.end(), crange.start());
                    }
                }
                return bounds;
            }},
        clustering_condition);
}

value_list in_values::get_value_set(const query_options& options) const {
    cql3::raw_value in_values_list_bytes = evaluate(in_values_list, options);
    utils::chunked_vector<managed_bytes> in_list_elems = get_list_elements(in_values_list_bytes);
    std::vector<managed_bytes> vals;
    vals.reserve(in_list_elems.size());
    for (managed_bytes& elem : in_list_elems) {
        vals.push_back(std::move(elem));
    }
    return vals;
}

nonwrapping_range<managed_bytes> range_bound::get_value_set(const query_options& options) const {
    if (!value.has_value()) {
        return nonwrapping_range<managed_bytes>::make_open_ended_both_sides();
    }

    managed_bytes bound_val = evaluate(*value, options).to_managed_bytes();
    interval_bound bound(std::move(bound_val), inclusive);
    if (type == range_bound::type::upper) {
        return nonwrapping_range<managed_bytes>::make_ending_with(std::move(bound));
    } else {
        return nonwrapping_range<managed_bytes>::make_starting_with(std::move(bound));
    }
}

value_list value_list_conditions::get_value_set(const query_options& options) const {
    value_list result = in_list.get_value_set(options);
    const list_type_impl* in_list_type =
        dynamic_cast<const list_type_impl*>(&type_of(in_list.in_values_list)->without_reversed());
    const abstract_type* values_type = in_list_type->get_elements_type().get();

    for (const std::variant<in_values, range_bound>& condition : other_conditions) {
        value_set condition_values = std::visit(
            overloaded_functor{[&](const in_values& in_vals) -> value_set { return in_vals.get_value_set(options); },
                               [&](const range_bound& range) -> value_set { return range.get_value_set(options); }},
            condition);
        value_set new_result = expr::intersection(std::move(result), std::move(condition_values), values_type);
        if (!std::holds_alternative<value_list>(new_result)) {
            throw std::runtime_error("Intersection of values and ranges gave range :O - thats impossible");
        }
        result = std::move(std::get<value_list>(new_result));
    }
    return result;
}

nonwrapping_range<managed_bytes> range_conditions::get_value_set(const query_options& options) const {
    std::optional<nonwrapping_range<managed_bytes>> result;

    for (const range_bound& range : ranges) {
        if (!range.value.has_value()) {
            // unbounded, just ignore
            continue;
        }

        nonwrapping_range<managed_bytes> cur_values = range.get_value_set(options);
        if (!result.has_value()) {
            result = std::move(cur_values);
            continue;
        }

        const abstract_type* values_type =
            &type_of(*range.value)->without_reversed();  // TODO: - without reversed or not?
        value_set intersected = expr::intersection(std::move(*result), std::move(cur_values), values_type);
        if (!std::holds_alternative<nonwrapping_range<managed_bytes>>(intersected)) {
            throw std::runtime_error("Intersection of ranges is not a range!");
        }
        result = std::move(std::get<nonwrapping_range<managed_bytes>>(intersected));
    }

    if (!result.has_value()) {
        return nonwrapping_range<managed_bytes>::make_open_ended_both_sides();
    }

    return std::move(*result);
}

planned_query query_from_statement_restrictions(const restrictions::statement_restrictions& stmt_restrictions) {
    if (stmt_restrictions.uses_secondary_indexing()) {
        throw std::runtime_error("planned_query_from_statement_restrictions handles only non-index queries");
    }

    return single_table_query_from_restrictions(stmt_restrictions);
}
}  // namespace plan

namespace restrictions {
refactor_restrictions::refactor_restrictions(new_restrictions single_table_plan)
    : restrictions(std::move(single_table_plan)) {}

refactor_restrictions::refactor_restrictions(old_restrictions old_restrs) : restrictions(std::move(old_restrs)) {}

dht::partition_range_vector refactor_restrictions::get_partition_key_ranges(const query_options& options) const {
    return std::visit(
        overloaded_functor{
            [&](const new_restrictions& new_restrs) { return new_restrs.get_partition_key_ranges(options); },
            [&](const old_restrictions& old_restrs) { return old_restrs->get_partition_key_ranges(options); }},
        restrictions);
}
std::vector<query::clustering_range> refactor_restrictions::get_clustering_bounds(const query_options& options) const {
    return std::visit(
        overloaded_functor{
            [&](const new_restrictions& new_restrs) { return new_restrs.get_clustering_bounds(options); },
            [&](const old_restrictions& old_restrs) { return old_restrs->get_clustering_bounds(options); }},
        restrictions);
}

const expr::expression& refactor_restrictions::get_partition_key_restrictions() const {
    return std::visit(
        overloaded_functor{
            [&](const new_restrictions& new_restrs) { return new_restrs.partition_key_restrictions; },
            [&](const old_restrictions& old_restrs) { return old_restrs->get_partition_key_restrictions(); }},
        restrictions);
}

const expr::expression& refactor_restrictions::get_clustering_columns_restrictions() const {
    return std::visit(
        overloaded_functor{
            [&](const new_restrictions& new_restrs) { return new_restrs.clustering_key_restrictions; },
            [&](const old_restrictions& old_restrs) { return old_restrs->get_clustering_columns_restrictions(); }},
        restrictions);
}

const expr::single_column_restrictions_map& refactor_restrictions::get_non_pk_restriction() const {
    return std::visit(
        overloaded_functor{
            [&](const new_restrictions& new_restrs) { return new_restrs.nonprimary_key_restrictions_map; },
            [&](const old_restrictions& old_restrs) { return old_restrs->get_non_pk_restriction(); }},
        restrictions);
}

bool refactor_restrictions::has_partition_key_unrestricted_components() const {
    return std::visit(
        overloaded_functor{[&](const new_restrictions& new_restrs) {
                               return !std::holds_alternative<plan::no_condition>(new_restrs.partition_condition);
                           },
                           [&](const old_restrictions& old_restrs) {
                               return old_restrs->has_partition_key_unrestricted_components();
                           }},
        restrictions);
}

bool refactor_restrictions::has_token_restrictions() const {
    return std::visit(
        overloaded_functor{
            [&](const new_restrictions& new_restrs) { return has_token(new_restrs.partition_key_restrictions); },
            [&](const old_restrictions& old_restrs) { return old_restrs->has_token_restrictions(); }},
        restrictions);
}

std::vector<query::clustering_range> refactor_restrictions::get_global_index_clustering_ranges(
    const query_options& options,
    const schema& idx_tbl_schema) const {
    return std::visit(
        overloaded_functor{[&](const new_restrictions& new_restrs) -> std::vector<query::clustering_range> {
                               throw std::runtime_error(
                                   "get_global_index_clustering_ranges not implemented for new_restrictions");
                           },
                           [&](const old_restrictions& old_restrs) -> std::vector<query::clustering_range> {
                               return old_restrs->get_global_index_clustering_ranges(options, idx_tbl_schema);
                           }},
        restrictions);
}

/// Calculates clustering ranges for querying a global-index table for queries with token restrictions present.
std::vector<query::clustering_range> refactor_restrictions::get_global_index_token_clustering_ranges(
    const query_options& options,
    const schema& idx_tbl_schema) const {
    return std::visit(
        overloaded_functor{[&](const new_restrictions& new_restrs) -> std::vector<query::clustering_range> {
                               throw std::runtime_error(
                                   "get_global_index_token_clustering_ranges not implemented for new_restrictions");
                           },
                           [&](const old_restrictions& old_restrs) -> std::vector<query::clustering_range> {
                               return old_restrs->get_global_index_token_clustering_ranges(options, idx_tbl_schema);
                           }},
        restrictions);
}

std::vector<query::clustering_range> refactor_restrictions::get_local_index_clustering_ranges(
    const query_options& options,
    const schema& idx_tbl_schema) const {
    return std::visit(
        overloaded_functor{[&](const new_restrictions& new_restrs) -> std::vector<query::clustering_range> {
                               throw std::runtime_error(
                                   "get_local_index_clustering_ranges not implemented for new_restrictions");
                           },
                           [&](const old_restrictions& old_restrs) -> std::vector<query::clustering_range> {
                               return old_restrs->get_local_index_clustering_ranges(options, idx_tbl_schema);
                           }},
        restrictions);
}

bool refactor_restrictions::need_filtering() const {
    return std::visit(
        overloaded_functor{[&](const new_restrictions& new_restrs) {
                               return new_restrs.partition_filter.has_value() ||
                                      new_restrs.clustering_filter.has_value() || new_restrs.other_filter.has_value();
                           },
                           [&](const old_restrictions& old_restrs) { return old_restrs->need_filtering(); }},
        restrictions);
}

bool refactor_restrictions::uses_secondary_indexing() const {
    return std::visit(
        overloaded_functor{[&](const new_restrictions& new_restrs) { return false; },
                           [&](const old_restrictions& old_restrs) { return old_restrs->uses_secondary_indexing(); }},
        restrictions);
}

bool refactor_restrictions::has_non_primary_key_restriction() const {
    return std::visit(
        overloaded_functor{
            [&](const new_restrictions& new_restrs) { return new_restrs.other_filter.has_value(); },
            [&](const old_restrictions& old_restrs) { return old_restrs->has_non_primary_key_restriction(); }},
        restrictions);
}

bool refactor_restrictions::has_clustering_columns_restriction() const {
    return std::visit(overloaded_functor{[&](const new_restrictions& new_restrs) {
                                             return !is_empty_restriction(new_restrs.clustering_key_restrictions);
                                         },
                                         [&](const old_restrictions& old_restrs) {
                                             return old_restrs->has_clustering_columns_restriction();
                                         }},
                      restrictions);
}

bool refactor_restrictions::is_key_range() const {
    return std::visit(
        overloaded_functor{[&](const new_restrictions& new_restrs) {
                               return std::visit(overloaded_functor{
                                                     [](const plan::no_condition&) { return true; },
                                                     [](const plan::column_value_lists&) { return false; },
                                                     [](const plan::token_value_list&) { return false; },
                                                     [](const plan::token_range&) { return true; },
                                                 },
                                                 new_restrs.partition_condition);
                           },
                           [&](const old_restrictions& old_restrs) { return old_restrs->is_key_range(); }},
        restrictions);
}

std::vector<const column_definition*> refactor_restrictions::get_column_defs_for_filtering(
    data_dictionary::database db) const {
    return std::visit(
        overloaded_functor{
            [&](const new_restrictions& new_restrs) -> std::vector<const column_definition*> {
                std::set<const column_definition*, schema_pos_column_definition_comparator> pk_columns_for_filtering;
                std::set<const column_definition*, schema_pos_column_definition_comparator> ck_columns_for_filtering;
                std::set<const column_definition*, schema_pos_column_definition_comparator> other_columns_for_filtering;

                if (new_restrs.partition_filter.has_value()) {
                    for_each_expression<column_value>(
                        new_restrs.partition_filter->filter_expr,
                        [&](const column_value& cval) { pk_columns_for_filtering.insert(cval.col); });
                }

                if (new_restrs.clustering_filter.has_value()) {
                    for_each_expression<column_value>(
                        new_restrs.clustering_filter->filter_expr,
                        [&](const column_value& cval) { ck_columns_for_filtering.insert(cval.col); });
                }

                if (new_restrs.other_filter.has_value()) {
                    for_each_expression<column_value>(
                        new_restrs.other_filter->filter_expr,
                        [&](const column_value& cval) { other_columns_for_filtering.insert(cval.col); });
                }

                std::vector<const column_definition*> result;
                for (const column_definition* cdef : pk_columns_for_filtering) {
                    result.push_back(cdef);
                }
                for (const column_definition* cdef : ck_columns_for_filtering) {
                    result.push_back(cdef);
                }
                for (const column_definition* cdef : other_columns_for_filtering) {
                    result.push_back(cdef);
                }
                return result;
            },
            [&](const old_restrictions& old_restrs) -> std::vector<const column_definition*> {
                return old_restrs->get_column_defs_for_filtering(db);
            }},
        restrictions);
}

bool refactor_restrictions::partition_key_restrictions_is_empty() const {
    return std::visit(
        overloaded_functor{
            [&](const new_restrictions& new_restrs) {
                return std::holds_alternative<plan::no_condition>(new_restrs.partition_condition) &&
                       !new_restrs.partition_filter.has_value();
            },
            [&](const old_restrictions& old_restrs) { return old_restrs->partition_key_restrictions_is_empty(); }},
        restrictions);
}

bool refactor_restrictions::partition_key_restrictions_is_all_eq() const {
    return std::visit(
        overloaded_functor{
            [&](const new_restrictions& new_restrs) {
                return std::holds_alternative<plan::column_value_lists>(new_restrs.partition_condition) &&
                       !new_restrs.partition_filter.has_value();
            },
            [&](const old_restrictions& old_restrs) { return old_restrs->partition_key_restrictions_is_all_eq(); }},
        restrictions);
}

bool refactor_restrictions::key_is_in_relation() const {
    return std::visit(
        overloaded_functor{
            [&](const new_restrictions& new_restrs) {
                return find(new_restrs.partition_key_restrictions, expr::oper_t::IN) != nullptr;
            },
            [&](const old_restrictions& old_restrs) { return old_restrs->partition_key_restrictions_is_all_eq(); }},
        restrictions);
}

/// Prepares internal data for evaluating index-table queries.  Must be called before
/// get_local_index_clustering_ranges().
void refactor_restrictions::prepare_indexed_local(const schema& idx_tbl_schema) {
    std::visit(
        overloaded_functor{[&](new_restrictions& new_restrs) {
                               throw std::runtime_error("prepare_indexed_local not implemented for new_restrictions");
                           },
                           [&](old_restrictions& old_restrs) { old_restrs->prepare_indexed_local(idx_tbl_schema); }},
        restrictions);
}

/// Prepares internal data for evaluating index-table queries.  Must be called before
/// get_global_index_clustering_ranges() or get_global_index_token_clustering_ranges().
void refactor_restrictions::prepare_indexed_global(const schema& idx_tbl_schema) {
    std::visit(
        overloaded_functor{[&](new_restrictions& new_restrs) {
                               throw std::runtime_error("prepare_indexed_global not implemented for new_restrictions");
                           },
                           [&](old_restrictions& old_restrs) { old_restrs->prepare_indexed_global(idx_tbl_schema); }},
        restrictions);
}

std::pair<std::optional<secondary_index::index>, expr::expression> refactor_restrictions::find_idx(
    const secondary_index::secondary_index_manager& sim) const {
    return std::visit(
        overloaded_functor{
            [&](new_restrictions& new_restrs) -> std::pair<std::optional<secondary_index::index>, expr::expression> {
                throw std::runtime_error("find_idx not implemented for new_restrictions");
            },
            [&](old_restrictions& old_restrs) -> std::pair<std::optional<secondary_index::index>, expr::expression> {
                return old_restrs->find_idx(sim);
            }},
        restrictions);
}

bool refactor_restrictions::has_eq_restriction_on_column(const column_definition& column) const {
    return std::visit(overloaded_functor{[&](const new_restrictions& new_restrs) {
                                             return expr::has_eq_restriction_on_column(column, new_restrs.where);
                                         },
                                         [&](const old_restrictions& old_restrs) {
                                             return old_restrs->has_eq_restriction_on_column(column);
                                         }},
                      restrictions);
}

bool refactor_restrictions::pk_restrictions_need_filtering() const {
    return std::visit(
        overloaded_functor{
            [&](const new_restrictions& new_restrs) { return !new_restrs.partition_filter.has_value(); },
            [&](const old_restrictions& old_restrs) { return old_restrs->pk_restrictions_need_filtering(); }},
        restrictions);
}

bool refactor_restrictions::ck_restrictions_need_filtering() const {
    return std::visit(
        overloaded_functor{
            [&](const new_restrictions& new_restrs) { return !new_restrs.clustering_filter.has_value(); },
            [&](const old_restrictions& old_restrs) { return old_restrs->ck_restrictions_need_filtering(); }},
        restrictions);
}

const expr::single_column_restrictions_map& refactor_restrictions::get_single_column_partition_key_restrictions()
    const {
    return std::visit(overloaded_functor{
                          [&](const new_restrictions& new_restrs) { return new_restrs.partition_key_restrictions_map; },
                          [&](const old_restrictions& old_restrs) {
                              return old_restrs->get_single_column_partition_key_restrictions();
                          }},
                      restrictions);
}

const expr::single_column_restrictions_map& refactor_restrictions::get_single_column_clustering_key_restrictions()
    const {
    return std::visit(overloaded_functor{[&](const new_restrictions& new_restrs) {
                                             return new_restrs.clustering_key_restrictions_map;
                                         },
                                         [&](const old_restrictions& old_restrs) {
                                             return old_restrs->get_single_column_clustering_key_restrictions();
                                         }},
                      restrictions);
}

bool refactor_restrictions::has_unrestricted_clustering_columns() const {
    return std::visit(
        overloaded_functor{
            [&](const new_restrictions& new_restrs) {
                std::set<const column_definition*> cdefs;
                for_each_expression<column_value>(new_restrs.clustering_key_restrictions,
                                                  [&](const column_value& cval) { cdefs.insert(cval.col); });
                return cdefs.size() < new_restrs.table_schema->clustering_key_size();
            },
            [&](const old_restrictions& old_restrs) { return old_restrs->has_unrestricted_clustering_columns(); }},
        restrictions);
}

bool refactor_restrictions::is_restricted(const column_definition* cdef) const {
    return std::visit(
        overloaded_functor{[&](const new_restrictions& new_restrs) {
                               return find_in_expression<column_value>(new_restrs.where, [&](const column_value& cval) {
                                          return cval.col == cdef;
                                      }) != nullptr;
                           },
                           [&](const old_restrictions& old_restrs) { return old_restrs->is_restricted(cdef); }},
        restrictions);
}
}  // namespace restrictions
}  // namespace cql3
