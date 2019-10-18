#pragma once
#include "common.h"
#include <unordered_map>

namespace openset::errors
{
    enum class errorClass_e : int32_t {
        no_error = 0,
        config,
        parse,
        run_time,
        insert,
        query,
        internode
    };

    enum class errorCode_e : int32_t
    {
        // no error
        no_error = 0,

        // compiler errors
        syntax_error = 10000,
        general_error,
        general_config_error,
        general_query_error,
        syntax_indentation,
        template_missing_var,
        property_not_found_schema,
        syntax_groupby,
        syntax_column_dereference,
        syntax_in_clause,
        syntax_missing_subscript,
        syntax_missing_quotes_on_property,
        record_param_must_be_in_group,
        property_not_in_table,
        property_already_referenced,
        missing_function_definition,
        sdk_param_count,
        missing_function_entry_point,
        exec_count_exceeded,
        date_parse_error,
        date_range_and_expected,
        date_within_malformed,
        date_diff_invalid,
        could_not_open_trigger,
        iteration_error,
        set_math_param_invalid,
        recursion,
        run_time_exception_triggered,
        parse_time_exception_triggered,
        internode_error,
        break_depth_to_deep,
        partition_migrated,
        route_error,
        item_not_found
    };
};

namespace std {
    template<>
    struct hash<openset::errors::errorClass_e> {
        size_t operator()(const openset::errors::errorClass_e &v) const {
            return static_cast<size_t>(v);
        }
    };

    template<>
    struct hash<openset::errors::errorCode_e> {
        size_t operator()(const openset::errors::errorCode_e &v) const {
            return static_cast<size_t>(v);
        }
    };

};

namespace openset::errors
{
    static const std::unordered_map<errorClass_e, std::string> classStrings = {
        {errorClass_e::no_error, "no_error"},
        { errorClass_e::config, "config" },
        {errorClass_e::parse,    "parse"},
        {errorClass_e::run_time, "run_time"},
        {errorClass_e::insert,   "insert"},
        { errorClass_e::query,   "query" },
        { errorClass_e::internode, "internode"}
    };


    static const std::unordered_map<errorCode_e, std::string> errorStrings = {
        { errorCode_e::no_error, "no_error"},
        { errorCode_e::syntax_error, "syntax_error" },
        { errorCode_e::general_error, "general_error" }, // a WTF error
        { errorCode_e::general_config_error, "general_config_error" },
        { errorCode_e::general_query_error, "general_query_error" },
        { errorCode_e::syntax_indentation, "indentification_error - must be 4 spaces" },
        { errorCode_e::template_missing_var, "template_missing_var" },
        { errorCode_e::property_not_found_schema, "property_not_found_schema" },
        { errorCode_e::syntax_groupby, "syntax_syntax" },
        { errorCode_e::syntax_in_clause, "syntax_in_clause" },
        { errorCode_e::syntax_missing_subscript, "syntax_missing_subscript" },
        { errorCode_e::syntax_missing_quotes_on_property, "syntax_missing_quotes_on_property" },
        { errorCode_e::syntax_column_dereference, "syntax_column_dereference" },
        { errorCode_e::record_param_must_be_in_group, "record_param_must_be_in_groupby" },
        { errorCode_e::property_not_in_table, "column_not_in_schema" },
        { errorCode_e::missing_function_definition, "missing_function_definition" },
        { errorCode_e::sdk_param_count, "sdk_param_count"},
        { errorCode_e::property_already_referenced, "property_already_referenced" },
        { errorCode_e::missing_function_entry_point, "missing_function_entry_point"},
        { errorCode_e::exec_count_exceeded, "exec_count_exceeded"},
        { errorCode_e::date_parse_error, "date_parse_error - expecting ISO 8601"},
        { errorCode_e::date_range_and_expected, "date_range_and_expected"},
        { errorCode_e::date_within_malformed, "date_within_malformed"},
        { errorCode_e::could_not_open_trigger, "could_not_open_trigger"},
        { errorCode_e::iteration_error, "iteration error"},
        { errorCode_e::set_math_param_invalid, "set_math_param_invalid"},
        { errorCode_e::recursion, "an error in the code caused a recursive loop"},
        { errorCode_e::run_time_exception_triggered, "run_time_exception_triggered"},
        { errorCode_e::parse_time_exception_triggered, "parse_time_exception_triggered"},
        { errorCode_e::internode_error, "internode_error"},
        { errorCode_e::break_depth_to_deep, "break ## to deep for current nest level"},
        { errorCode_e::partition_migrated, "parition migrated. Task could not be completed."},
        { errorCode_e::route_error, "route not found (node down?)"},
        { errorCode_e::item_not_found, "item not found"}
    };

    class Error
    {
    private:
        errorCode_e eCode { errorCode_e::no_error };
        errorClass_e eClass { errorClass_e::no_error };
        std::string message;
        std::string classMessage;
        std::string detail;
        std::string additional;
    public:
        Error();
        Error(
            const errorClass_e errorClass,
            const errorCode_e errorCode,
            const std::string errorDetail,
            const std::string errorAdditional = "");

        ~Error();

        void set(
            const errorClass_e errorClass,
            const errorCode_e errorCode,
            const std::string errorDetail,
            const std::string errorAdditional = "");

        bool inError() const;
        std::string getErrorJSON() const;
    };

};