#include "cql3/expr/expression.hh"
#include "index/secondary_index_manager.hh"
#include "statement_restrictions.hh"

namespace cql3 {
namespace plan {

struct no_condition {};

struct in_values {
    expr::expression in_values_list;

    expr::value_list get_value_set(const query_options& options) const;
};

struct range_bound {
    enum struct type { upper, lower };

    type type;
    std::optional<expr::expression> value;
    bool inclusive;
    expr::comparison_order order;

    nonwrapping_range<managed_bytes> get_value_set(const query_options& options) const;
};

// Collection of conditions that produce a limited number of values in a list
struct value_list_conditions {
    in_values in_list;
    std::vector<std::variant<in_values, range_bound>> other_conditions;

    expr::value_list get_value_set(const query_options& options) const;
};

// Collection of condtitions that produce a range of values
struct range_conditions {
    std::vector<range_bound> ranges;

    nonwrapping_range<managed_bytes> get_value_set(const query_options& options) const;
};

struct column_value_lists {
    std::map<const column_definition*, value_list_conditions, expr::schema_pos_column_definition_comparator>
        column_values;
};

struct token_value_list {
    value_list_conditions values;
};

struct token_range {
    range_conditions ranges;
};

struct multi_column_in_values {
    std::vector<const column_definition*> columns;
    expr::expression in_values;
};

struct multi_column_clustering_range {
    std::vector<const column_definition*> clustering_prefix;
    range_conditions prefix_range;
};

struct single_column_clustering_range {
    column_value_lists prefix_column_values;

    const column_definition* last_prefix_column;
    range_conditions last_column_range;
};

struct filter {
    expr::expression filter_expr;
};

using partition_key_condition = std::variant<no_condition, column_value_lists, token_value_list, token_range>;
using clustering_key_condition = std::variant<no_condition,
                                              column_value_lists,
                                              multi_column_in_values,
                                              single_column_clustering_range,
                                              multi_column_clustering_range>;

struct single_table_query {
    schema_ptr table_schema;

    partition_key_condition partition_condition;
    std::optional<filter> partition_filter;

    clustering_key_condition clustering_condition;
    std::optional<filter> clustering_filter;

    std::optional<filter> other_filter;

    dht::partition_range_vector get_partition_key_ranges(const query_options& options) const;
    std::vector<query::clustering_range> get_clustering_bounds(const query_options& options) const;
};

struct global_index_query {
    secondary_index::index index;
    single_table_query index_query;
};

struct local_index_query {
    secondary_index::index index;
    single_table_query index_query;
};

using planned_query = std::variant<single_table_query, local_index_query, global_index_query>;

planned_query query_from_statement_restrictions(const restrictions::statement_restrictions&);
}  // namespace plan
}  // namespace cql3