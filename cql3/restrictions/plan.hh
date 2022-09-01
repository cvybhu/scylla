#pragma once

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

    expr::expression where;
    expr::expression partition_key_restrictions;
    expr::single_column_restrictions_map partition_key_restrictions_map;

    partition_key_condition partition_condition;
    std::optional<filter> partition_filter;

    expr::expression clustering_key_restrictions;
    expr::single_column_restrictions_map clustering_key_restrictions_map;

    clustering_key_condition clustering_condition;
    std::optional<filter> clustering_filter;

    expr::expression nonprimary_key_restrictions;
    expr::single_column_restrictions_map nonprimary_key_restrictions_map;
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

namespace restrictions {
class refactor_restrictions {
   public:
    using old_restrictions = const ::shared_ptr<restrictions::statement_restrictions>;
    using new_restrictions = const plan::single_table_query;
    std::variant<old_restrictions, new_restrictions> restrictions;

    refactor_restrictions(new_restrictions single_table_plan) ;
    refactor_restrictions(old_restrictions old_restrictions) ;

    dht::partition_range_vector get_partition_key_ranges(const query_options& options) const;
    std::vector<query::clustering_range> get_clustering_bounds(const query_options& options) const;

    const expr::expression& get_partition_key_restrictions() const ;

    const expr::expression& get_clustering_columns_restrictions() const ;

    const expr::single_column_restrictions_map& get_non_pk_restriction() const;

    bool has_partition_key_unrestricted_components() const ;

    bool has_token_restrictions() const ;

    std::vector<query::clustering_range> get_global_index_clustering_ranges(const query_options& options,
                                                                            const schema& idx_tbl_schema) const ;

    /// Calculates clustering ranges for querying a global-index table for queries with token restrictions present.
    std::vector<query::clustering_range> get_global_index_token_clustering_ranges(const query_options& options,
                                                                                  const schema& idx_tbl_schema) const ;

    std::vector<query::clustering_range> get_local_index_clustering_ranges(const query_options& options,
                                                                           const schema& idx_tbl_schema) const ;

    bool need_filtering() const ;

    bool uses_secondary_indexing() const ;

    bool has_non_primary_key_restriction() const ;

    bool has_clustering_columns_restriction() const ;

    bool is_key_range() const;

    std::vector<const column_definition*> get_column_defs_for_filtering(data_dictionary::database db) const;

    bool partition_key_restrictions_is_empty() const;

    bool partition_key_restrictions_is_all_eq() const;

    bool key_is_in_relation() const;

    /// Prepares internal data for evaluating index-table queries.  Must be called before
    /// get_local_index_clustering_ranges().
    void prepare_indexed_local(const schema& idx_tbl_schema);

    /// Prepares internal data for evaluating index-table queries.  Must be called before
    /// get_global_index_clustering_ranges() or get_global_index_token_clustering_ranges().
    void prepare_indexed_global(const schema& idx_tbl_schema);

    std::pair<std::optional<secondary_index::index>, expr::expression> find_idx(
        const secondary_index::secondary_index_manager& sim) const;

    bool has_eq_restriction_on_column(const column_definition& column) const;

    bool pk_restrictions_need_filtering() const;

    bool ck_restrictions_need_filtering() const;

    const expr::single_column_restrictions_map& get_single_column_partition_key_restrictions() const;
    const expr::single_column_restrictions_map& get_single_column_clustering_key_restrictions() const;
    bool has_unrestricted_clustering_columns() const;
    bool is_restricted(const column_definition* cdef) const;
};
}  // namespace restrictions
}  // namespace cql3