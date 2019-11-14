#pragma once
#include "common.h"
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include "errors.h"
#include "dbtypes.h"
#include "attributes.h"
#include "var/var.h"
#include "../lib/str/strtools.h"

namespace openset
{
    namespace query
    {
        enum class BlockType_e
        {
            code,
            lambda,
            function
        };

        // Result Column Modifiers
        enum class Modifiers_e : int32_t
        {
            sum,
            min,
            max,
            avg,
            count,
            dist_count_person,
            value,
            var,
            second_number,
            second_date,
            minute_number,
            minute_date,
            hour_number,
            hour_date,
            day_date,
            day_of_week,
            day_of_month,
            day_of_year,
            week_date,
            month_date,
            month_number,
            quarter_number,
            quarter_date,
            year_number,
            year_date,
            //lambda,
        };

        enum class OpCode_e : int32_t
        {
            NOP = 0,     // No operation
            LAMBDA,      // lambda
            PSHTBLCOL,   // push property
            PSHTBLFLT,   // push property
            PSHRESCOL,   // push result Column (may be grid, may be variable)
            //PSHRESGRP, // push group_by (may be grid, may be variable)
            VARIDX,      // placeholder for an index to a variable
            COLIDX,      // placeholder for an index to a property
            PSHPAIR,     // pushes a single pair dict to the stack
            PSHUSROBJ,   // push object with deref
            PSHUSROREF,  // push object with deref
            PSHUSRVAR,   // push var
            PSHUSRVREF,  // push pointer to variable (reference)
            PSHLITTRUE,  // push True
            PSHLITFALSE, // push False
            PSHLITSTR,   // push string
            PSHLITINT,   // push integer
            PSHLITFLT,   // push double
            PSHLITNUL,   // push None value (NONE)
            POPUSROBJ,   // pop object with deref
            POPUSRVAR,   // pop variable
            POPTBLCOL,   // pop property (place holder)
            POPRESGRP,   // pop current group value
            POPRESCOL,   // pop current result Column value
            CNDIF,       // condition if
            CNDELIF,     // condition else if
            CNDELSE,     // condition else
            ITFORR,      // row iterator
            ITFORRC,     // row iterator in continue mode
            ITFORRCF,    // row itetorar continue FROM mode
            ITFOR,       // for iterator'
            ITRFORR,     // reverse row iterator
            ITRFORRC,    // reverse row iterator in continue mode
            ITRFORRCF,   // reverse row itetorar continue FROM mode
            ITRFOR,      // for iterator'
            SETROW,      // set the row pointer to a specific row
            MATHADD,     // and last two stack items
            MATHSUB,     // sub last two stack items
            MATHMUL,     // multiply last two stack items
            MATHDIV,     // divide last two stack items
            MATHADDEQ,   // +=
            MATHSUBEQ,   // -=
            MATHMULEQ,   // *=
            MATHDIVEQ,   // /=
            OPGT,        // >
            OPLT,        // <
            OPGTE,       // >=
            OPLTE,       // <-
            OPEQ,        // == =
            OPNEQ,       // != <>
            OPWTHN,      // fuzzy range +/-
            OPNOT,       // !
            OPCONT,      // left contains all of right
            OPANY,       // left contains any contains any right
            OPIN,        // left in right
            LGCAND,      // logical and
            LGCOR,       // logical or
            MARSHAL,     // Marshal an internal C++ function
            CALL,        // Call a script defined function
            CALL_FOR,    // Call in `for` loop
            CALL_EACH,   // Call in `each` loop
            CALL_IF,     // Call `if`
            CALL_SUM,
            CALL_AVG,
            CALL_MIN,
            CALL_MAX,
            CALL_CNT,
            CALL_DCNT,
            CALL_TST,
            CALL_ROW,
            RETURN,      // Pops the call stack leaves last item on stack
            TERM,        // this script is done
            LGCNSTAND,
            LGCNSTOR
        }; // Marshal Functions
        enum class Marshals_e : int64_t
        {
            marshal_tally,
            marshal_now,
            marshal_row,
            marshal_last_stamp,
            marshal_first_stamp,
            marshal_bucket,
            marshal_round,
            marshal_trunc,
            marshal_fix,
            marshal_iso8601_to_stamp,
            marshal_to_seconds,
            marshal_to_minutes,
            marshal_to_hours,
            marshal_to_days,
            marshal_to_weeks,
            marshal_get_second,
            marshal_round_second,
            marshal_get_minute,
            marshal_round_minute,
            marshal_get_hour,
            marshal_round_hour,
            marshal_round_day,
            marshal_get_day_of_week,
            marshal_get_day_of_month,
            marshal_get_day_of_year,
            marshal_round_week,
            marshal_round_month,
            marshal_get_month,
            marshal_get_quarter,
            marshal_round_quarter,
            marshal_get_year,
            marshal_round_year,
            marshal_row_count,
            marshal_population,
            marshal_intersection,
            marshal_union,
            marshal_compliment,
            marshal_difference,
            marshal_session_count,
            marshal_return,
            marshal_break,
            marshal_continue,
            marshal_log,
            marshal_debug,
            marshal_exit,
            marshal_init_dict,
            marshal_init_list,
            marshal_make_dict,
            marshal_make_list,
            marshal_pop_subscript,
            marshal_push_subscript,
            marshal_set,
            marshal_list,
            marshal_dict,
            marshal_int,
            marshal_float,
            marshal_str,
            marshal_len,
            marshal_append,
            marshal_update,
            marshal_add,
            marshal_remove,
            marshal_del,
            marshal_contains,
            marshal_not_contains,
            marshal_pop,
            marshal_clear,
            marshal_keys,
            marshal_range,
            // not implemented
            marshal_str_split,
            marshal_str_find,
            marshal_str_rfind,
            marshal_str_replace,
            marshal_str_slice,
            marshal_str_strip,
            marshal_url_decode,
            marshal_get_row,
            marshal_eval_column_filter
        }; // enum used for query index optimizer
        enum class HintOp_e : int64_t
        {
            UNSUPPORTED,
            EQ,
            NEQ,
            GT,
            GTE,
            LT,
            LTE,
            PUSH_VAL,
            PUSH_TBL,
            BIT_OR,
            BIT_AND,

        };
    }
}

namespace std
{
    /*
     * RANT - being these enums are classes of POD types, they should have automatic
     *        hashing functions in GCC (they do in VC15+)
     */
    template <>
    struct hash<openset::query::Modifiers_e>
    {
        size_t operator()(const openset::query::Modifiers_e& v) const
        {
            return static_cast<size_t>(v);
        }
    };

    template <>
    struct hash<openset::query::OpCode_e>
    {
        size_t operator()(const openset::query::OpCode_e& v) const
        {
            return static_cast<size_t>(v);
        }
    };

    template <>
    struct hash<openset::query::HintOp_e>
    {
        size_t operator()(const openset::query::HintOp_e& v) const
        {
            return static_cast<size_t>(v);
        }
    };

    template <>
    struct hash<openset::query::Marshals_e>
    {
        size_t operator()(const openset::query::Marshals_e& v) const
        {
            return static_cast<size_t>(v);
        }
    };
};

namespace openset
{
    namespace query
    {
        // String to Result Properties Modifier
        static const unordered_map<string, int64_t> TimeConstants = {
            { "seconds", 1'000 },
            { "second", 1'000 },
            { "minute", 60'000 },
            { "minutes", 60'000 },
            { "hour", 3'600'000 },
            { "hours", 3'600'000 },
            { "day", 86'400'000 },
            { "days", 86'400'000 }
        };

        enum class TimeSwitch_e : int
        {
            seconds,
            minutes,
            hours,
            days
        };

        static const unordered_map<string, TimeSwitch_e> TimeSwitchMap = {
            { "seconds", TimeSwitch_e::seconds },
            { "minutes", TimeSwitch_e::minutes },
            { "hours", TimeSwitch_e::hours },
            { "days", TimeSwitch_e::days }
        }; // String to Result Properties Modifier
        static const unordered_map<string, Modifiers_e> ColumnModifiers = {
            { "sum", Modifiers_e::sum },
            { "min", Modifiers_e::min },
            { "max", Modifiers_e::max },
            { "avg", Modifiers_e::avg },
            { "count", Modifiers_e::count },
            { "dist_count_person", Modifiers_e::dist_count_person },
            { "value", Modifiers_e::value },
            { "val", Modifiers_e::value },
            { "variable", Modifiers_e::var },
            { "var", Modifiers_e::var },
            //{ "lambda", Modifiers_e::lambda },
        }; // Modifier to String (for debug output)
        static const unordered_map<Modifiers_e, string> ModifierDebugStrings = {
            { Modifiers_e::sum, "SUM" },
            { Modifiers_e::min, "MIN" },
            { Modifiers_e::max, "MAX" },
            { Modifiers_e::avg, "AVG" },
            { Modifiers_e::count, "COUNT" },
            { Modifiers_e::dist_count_person, "DCNTPP" },
            { Modifiers_e::value, "VALUE" },
            { Modifiers_e::var, "VAR" },
            { Modifiers_e::second_number, "SECOND" },
            { Modifiers_e::second_date, "DT_SECOND" },
            { Modifiers_e::minute_number, "MINUTE" },
            { Modifiers_e::minute_date, "DT_MINUTE" },
            { Modifiers_e::hour_number, "HOUR" },
            { Modifiers_e::hour_date, "DT_HOUR" },
            { Modifiers_e::day_date, "DT_DAY" },
            { Modifiers_e::day_of_week, "DAY_OF_WEEK" },
            { Modifiers_e::day_of_month, "DAY_OF_MONTH" },
            { Modifiers_e::day_of_year, "DAY_OF_YEAR" },
            { Modifiers_e::week_date, "DT_WEEK" },
            { Modifiers_e::month_date, "DT_MONTH" },
            { Modifiers_e::month_number, "MONTH" },
            { Modifiers_e::quarter_number, "QUARTER" },
            { Modifiers_e::quarter_date, "DT_QUARTER" },
            { Modifiers_e::year_number, "YEAR" },
            { Modifiers_e::year_date, "DT_YEAR" },
            //{ Modifiers_e::lambda, "LAMBDA"}
        }; // opCode to String (for debug output)
        static const unordered_map<OpCode_e, string> OpDebugStrings = {
            { OpCode_e::NOP, "NOP" },
            { OpCode_e::LAMBDA, "LAMBDA" },
            { OpCode_e::PSHTBLCOL, "PSHTBLCOL" },
            { OpCode_e::PSHTBLFLT, "PSHTBLFLT"},
            { OpCode_e::PSHRESCOL, "PSHRESCOL" },
            { OpCode_e::VARIDX, "VARIDX" },
            { OpCode_e::COLIDX, "COLIDX" },
            { OpCode_e::PSHPAIR, "PSHPAIR" },
            { OpCode_e::PSHUSROBJ, "PSHUSROBJ" },
            { OpCode_e::PSHUSROREF, "PSHUSROREF" },
            { OpCode_e::PSHUSRVAR, "PSHUSRVAR" },
            { OpCode_e::PSHUSRVREF, "PSHUSRVREF" },
            { OpCode_e::PSHLITSTR, "PSHLITSTR" },
            { OpCode_e::PSHLITINT, "PSHLITINT" },
            { OpCode_e::PSHLITFLT, "PSHLITFLT" },
            { OpCode_e::PSHLITNUL, "PSHLITNUL" },
            { OpCode_e::PSHLITTRUE, "PSHLITTRUE" },
            { OpCode_e::PSHLITFALSE, "PSHLITFALSE" },
            { OpCode_e::POPUSROBJ, "POPUSROBJ" },
            { OpCode_e::POPUSRVAR, "POPUSRVAR" },
            { OpCode_e::POPTBLCOL, "POPTBLCOL" },
            { OpCode_e::POPRESGRP, "POPRESGRP" },
            { OpCode_e::POPRESCOL, "POPRESCOL" },
            { OpCode_e::CNDIF, "CNDIF" },
            { OpCode_e::CNDELIF, "CNDELIF" },
            { OpCode_e::CNDELSE, "CNDELSE" },
            { OpCode_e::ITFORR, "ITFORR" },
            { OpCode_e::ITFORRC, "ITFORRC" },
            { OpCode_e::ITFORRCF, "ITFORRCF" },
            { OpCode_e::ITRFORR, "ITRFORR" },
            { OpCode_e::ITRFORRC, "ITRFORRC" },
            { OpCode_e::ITRFORRCF, "ITRFORRCF" },
            { OpCode_e::ITFOR, "ITFOR" },
            { OpCode_e::SETROW, "SETROW" },
            { OpCode_e::MATHADD, "MATHADD" },
            { OpCode_e::MATHSUB, "MATHSUB" },
            { OpCode_e::MATHMUL, "MATHMUL" },
            { OpCode_e::MATHDIV, "MATHDIV" },
            { OpCode_e::MATHADDEQ, "OPADDEQ" },
            { OpCode_e::MATHSUBEQ, "OPSUBEQ" },
            { OpCode_e::MATHMULEQ, "OPMULEQ" },
            { OpCode_e::MATHDIVEQ, "OPDIVEQ" },
            { OpCode_e::OPGT, "OPGT" },
            { OpCode_e::OPLT, "OPLT" },
            { OpCode_e::OPGTE, "OPGTE" },
            { OpCode_e::OPLTE, "OPLTE" },
            { OpCode_e::OPEQ, "OPEQ" },
            { OpCode_e::OPNEQ, "OPNEQ" },
            { OpCode_e::OPWTHN, "OPWTHN" },
            { OpCode_e::OPNOT, "OPNOT" },
            { OpCode_e::LGCAND, "LGCAND" },
            { OpCode_e::LGCOR, "LGCOR" },
            { OpCode_e::OPCONT, "CONT" },
            { OpCode_e::OPANY, "ANY" },
            { OpCode_e::OPIN, "IN" },
            { OpCode_e::MARSHAL, "MARSHAL" },
            { OpCode_e::CALL, "CALL" },
            { OpCode_e::CALL_FOR, "CALLFOR" },
            { OpCode_e::CALL_EACH, "CALLEACH" },
            { OpCode_e::CALL_IF, "CALLIF" },
            { OpCode_e::CALL_SUM, "CALLSUM" },
            { OpCode_e::CALL_AVG, "CALLAVG" },
            { OpCode_e::CALL_MIN, "CALLMIN" },
            { OpCode_e::CALL_MAX, "CALLMAX" },
            { OpCode_e::CALL_CNT, "CALLCNT" },
            { OpCode_e::CALL_DCNT, "CALLDCNT" },
            { OpCode_e::CALL_TST, "CALLTST" },
            { OpCode_e::CALL_ROW, "CALLROW" },
            { OpCode_e::RETURN, "RETURN" },
            { OpCode_e::TERM, "TERM" }

        };
        static const unordered_map<string, Modifiers_e> TimeModifiers = {
            { "second", Modifiers_e::second_number },
            { "date_second", Modifiers_e::second_number },
            { "minute", Modifiers_e::minute_number },
            { "date_minute", Modifiers_e::minute_date },
            { "hour", Modifiers_e::hour_number },
            { "date_hour", Modifiers_e::hour_date },
            { "date_day", Modifiers_e::day_date },
            { "day_of_week", Modifiers_e::day_of_week },
            { "day_of_month", Modifiers_e::day_of_month },
            { "day_of_year", Modifiers_e::day_of_year },
            { "date_week", Modifiers_e::week_date },
            { "date_month", Modifiers_e::month_date },
            { "month", Modifiers_e::month_number },
            { "quarter", Modifiers_e::quarter_number },
            { "date_quarter", Modifiers_e::quarter_date },
            { "year", Modifiers_e::year_number },
            { "date_year", Modifiers_e::year_date },
        };

        static const unordered_set<Modifiers_e> isTimeModifiers = {};
        static const unordered_set<string> RedundantSugar       = {
            { "of" },
        };
        using MarshalSet = std::unordered_set<Marshals_e>; // Marshal maps
        static const unordered_map<string, Marshals_e> Marshals = {
            { "tally", Marshals_e::marshal_tally },
            { "now", Marshals_e::marshal_now },
            { "cursor", Marshals_e::marshal_row },
            { "last_stamp", Marshals_e::marshal_last_stamp },
            { "first_stamp", Marshals_e::marshal_first_stamp },
            { "bucket", Marshals_e::marshal_bucket },
            { "round", Marshals_e::marshal_round },
            { "trunc", Marshals_e::marshal_trunc },
            { "fix", Marshals_e::marshal_fix },
            { "iso8601_to_stamp", Marshals_e::marshal_iso8601_to_stamp },
            { "to_seconds", Marshals_e::marshal_to_seconds },
            { "to_minutes", Marshals_e::marshal_to_minutes },
            { "to_hours", Marshals_e::marshal_to_hours },
            { "to_days", Marshals_e::marshal_to_days },
            { "to_weeks", Marshals_e::marshal_to_weeks },
            { "get_second", Marshals_e::marshal_get_second },
            { "start_of_second", Marshals_e::marshal_round_second },
            { "get_minute", Marshals_e::marshal_get_minute },
            { "start_of_minute", Marshals_e::marshal_round_minute },
            { "get_hour", Marshals_e::marshal_get_hour },
            { "start_of_hour", Marshals_e::marshal_round_hour },
            { "date_day", Marshals_e::marshal_round_day },
            { "get_day_of_week", Marshals_e::marshal_get_day_of_week },
            { "get_day_of_month", Marshals_e::marshal_get_day_of_month },
            { "get_day_of_year", Marshals_e::marshal_get_day_of_year },
            { "start_of_week", Marshals_e::marshal_round_week },
            { "start_of_month", Marshals_e::marshal_round_month },
            { "get_month", Marshals_e::marshal_get_month },
            { "get_quarter", Marshals_e::marshal_get_quarter },
            { "start_of_quarter", Marshals_e::marshal_round_quarter },
            { "get_year", Marshals_e::marshal_get_year },
            { "start_of_year", Marshals_e::marshal_round_year },
            { "row_count", Marshals_e::marshal_row_count },
            { "population", Marshals_e::marshal_population },
            { "intersection", Marshals_e::marshal_intersection },
            { "union", Marshals_e::marshal_union },
            { "compliment", Marshals_e::marshal_compliment },
            { "difference", Marshals_e::marshal_difference },
            { "session_count", Marshals_e::marshal_session_count },
            { "return", Marshals_e::marshal_return },
            { "continue", Marshals_e::marshal_continue },
            { "break", Marshals_e::marshal_break },
            { "log", Marshals_e::marshal_log },
            { "debug", Marshals_e::marshal_debug },
            { "exit", Marshals_e::marshal_exit },
            { "__internal_init_dict", Marshals_e::marshal_init_dict },
            { "__internal_init_list", Marshals_e::marshal_init_list },
            { "set", Marshals_e::marshal_set },
            { "list", Marshals_e::marshal_list },
            { "dict", Marshals_e::marshal_dict },
            { "int", Marshals_e::marshal_int },
            { "float", Marshals_e::marshal_float },
            { "str", Marshals_e::marshal_str },
            { "__internal_make_dict", Marshals_e::marshal_make_dict },
            { "__internal_make_list", Marshals_e::marshal_make_list },
            { "__internal_pop_subscript", Marshals_e::marshal_pop_subscript },
            { "__internal_push_subscript", Marshals_e::marshal_push_subscript },
            { "len", Marshals_e::marshal_len },
            { "__append", Marshals_e::marshal_append },
            { "__update", Marshals_e::marshal_update },
            { "__add", Marshals_e::marshal_add },
            { "__remove", Marshals_e::marshal_remove },
            { "__del", Marshals_e::marshal_del },
            { "__contains", Marshals_e::marshal_contains },
            { "__notcontains", Marshals_e::marshal_not_contains },
            { "__pop", Marshals_e::marshal_pop },
            { "__clear", Marshals_e::marshal_clear },
            { "keys", Marshals_e::marshal_keys },
            { "__split", Marshals_e::marshal_str_split },
            { "__find", Marshals_e::marshal_str_find },
            { "__rfind", Marshals_e::marshal_str_rfind },
            { "__slice", Marshals_e::marshal_str_slice },
            { "__strip", Marshals_e::marshal_str_strip },
            { "range", Marshals_e::marshal_range },
            { "url_decode", Marshals_e::marshal_url_decode },
            { "get_row", Marshals_e::marshal_get_row },
            { "__eval_column_filter", Marshals_e::marshal_eval_column_filter}
        };
        static const unordered_set<Marshals_e> SegmentMathMarshals = {
            { Marshals_e::marshal_population },
            { Marshals_e::marshal_intersection },
            { Marshals_e::marshal_union },
            { Marshals_e::marshal_compliment },
            { Marshals_e::marshal_difference },
        };
        static const unordered_set<string> SessionMarshals = {
            "session_count",
        };

        // these are marshals that do not take params by default, so they appear
        // like variables.
        static const unordered_set<string> MacroMarshals = {
            { "now" },
            { "cursor" },
            { "last_stamp" },
            { "first_stamp" },
            { "session_count" },
            { "row_count" },
            { "break" },
            { "exit" },
            { "continue" },
            { "__internal_init_dict" },
            { "__internal_init_list" },
        };

        // Comparatives
        static const unordered_map<string, OpCode_e> Operators = {
            { ">=", OpCode_e::OPGTE },
            { "<=", OpCode_e::OPLTE },
            { ">", OpCode_e::OPGT },
            { "<", OpCode_e::OPLT },
            { "==", OpCode_e::OPEQ },
            { "=", OpCode_e::OPEQ },
            { "!=", OpCode_e::OPNEQ },
            { "<>", OpCode_e::OPNEQ },
            { "[!=]", OpCode_e::OPNEQ },
            { "[==]", OpCode_e::OPEQ },
            { "contains", OpCode_e::OPCONT },
            { "any", OpCode_e::OPANY },
            { "in", OpCode_e::OPIN },
        };
        static const unordered_map<string, OpCode_e> MathAssignmentOperators = {
            { "+=", OpCode_e::MATHADDEQ },
            // math operators
            { "-=", OpCode_e::MATHSUBEQ },
            { "*=", OpCode_e::MATHMULEQ },
            { "/=", OpCode_e::MATHDIVEQ },
        };
        static const unordered_map<OpCode_e, string> OperatorsDebug = {
            { OpCode_e::OPGTE, ">=" },
            { OpCode_e::OPLTE, "<=" },
            { OpCode_e::OPGT, ">" },
            { OpCode_e::OPLT, "<" },
            { OpCode_e::OPEQ, "==" },
            { OpCode_e::OPNEQ, "!=" },
            { OpCode_e::OPNOT, "!" }
        }; // Math
        static const unordered_map<string, OpCode_e> Math = {
            { "+", OpCode_e::MATHADD },
            { "-", OpCode_e::MATHSUB },
            { "*", OpCode_e::MATHMUL },
            { "/", OpCode_e::MATHDIV }
        }; // Conditionals
        static const unordered_map<string, OpCode_e> LogicalOperators = {
            { "&&", OpCode_e::LGCAND },
            { "||", OpCode_e::LGCOR },
        };
        static const unordered_map<OpCode_e, string> LogicalOperatorsDebug = {
            { OpCode_e::LGCAND, "and" },
            { OpCode_e::LGCOR, "or" },
        };
        static const unordered_map<HintOp_e, string> HintOperatorsDebug = {
            { HintOp_e::UNSUPPORTED, "UNSUP" },
            { HintOp_e::EQ, "EQ" },
            { HintOp_e::NEQ, "NEQ" },
            { HintOp_e::GT, "GT" },
            { HintOp_e::GTE, "GTE" },
            { HintOp_e::LT, "LT" },
            { HintOp_e::LTE, "LTE" },
            { HintOp_e::BIT_OR, "OR" },
            { HintOp_e::BIT_AND, "AND" },
            { HintOp_e::PUSH_VAL, "PSH_VAL" },
            { HintOp_e::PUSH_TBL, "PSH_TBL" },
        };
        static const unordered_map<std::string, HintOp_e> OpToHintOp = {
            { ">=", HintOp_e::GTE },
            { "<=", HintOp_e::LTE },
            { ">", HintOp_e::GT },
            { "<", HintOp_e::LT },
            { "==", HintOp_e::EQ },
            { "!=", HintOp_e::NEQ },
            { "[==]", HintOp_e::EQ },
            { "[!=]", HintOp_e::NEQ },
            { "&&", HintOp_e::BIT_AND },
            { "||", HintOp_e::BIT_OR }
        };

        struct HintOp_s
        {
            HintOp_e op;
            cvar value;
            int64_t hash {NONE};

            HintOp_s(const HintOp_e op, const int value)
                : op(op),
                  value(value),
                  hash(value)
            {}

            HintOp_s(const HintOp_e op, const int64_t value)
                : op(op),
                  value(value),
                  hash(value)
            {}

            HintOp_s(const HintOp_e op, const double value)
                : op(op),
                  value(value),
                  hash(static_cast<int64_t>(value * 1'000'000LL))
            {}

            HintOp_s(const HintOp_e op, const string& text)
                : op(op),
                  value(text),
                  hash(MakeHash(text))
            {}

            explicit HintOp_s(const HintOp_e op)
                : op(op),
                  value(0)
            {}
        };

        using HintOpList = vector<HintOp_s>;

        struct Variable_s
        {
            string actual;                               // actual name
            string alias;                                // alias
            string space;                                // namespace
            string distinctColumnName { "event" };       // name of property used for aggregators
            Modifiers_e modifier { Modifiers_e::value }; // default is value
            int index { -1 };                            // index
            int column { -1 };                           // property in grid
            int schemaColumn { -1 };                     // property in schema
            int distinctColumn { db::PROP_EVENT };        // property containing distinct key
            db::PropertyTypes_e schemaType { db::PropertyTypes_e::freeProp };
            bool isSet { false };
            bool isProp { false };
            bool isRowObject { false };
            bool aggOnce { false }; // customer props, distinct counts and value selects are counted once per branch per person in a result
            int popRefs { 0 };      // reference counter for pops
            int pushRefs { 0 };     // reference counter for pushes
            int sortOrder { -1 };   // used for sorting in property order
            int lambdaIndex { -1 }; // used for variable assignment by lambada
            bool nonDistinct { false };
            cvar value { NONE };
            cvar startingValue { NONE };
            Variable_s() = default;

            Variable_s(const string& actual, const string& space, const int sortOrder = -1)
                : actual(actual),
                  alias(actual),
                  space(space),
                  sortOrder(sortOrder)
            {}

            Variable_s(
                const string& actual,
                const string& alias,
                const string& space,
                const Modifiers_e modifier = Modifiers_e::value,
                const int sortOrder        = -1)
                : actual(actual),
                  alias(alias),
                  space(space),
                  modifier(modifier),
                  sortOrder(sortOrder)
            {}

            Variable_s(const Variable_s& source)
            {
                actual             = source.actual;
                alias              = source.alias;
                space              = source.space;
                distinctColumnName = source.distinctColumnName;
                distinctColumn     = source.distinctColumn;
                modifier           = source.modifier;
                index              = source.index;
                column             = source.column;
                schemaColumn       = source.schemaColumn;
                schemaType         = source.schemaType;
                isSet              = source.isSet;
                isProp             = source.isProp;
                isRowObject        = source.isRowObject;
                aggOnce            = source.aggOnce;
                popRefs            = source.popRefs;
                pushRefs           = source.pushRefs;
                sortOrder          = source.sortOrder;
                lambdaIndex        = source.lambdaIndex;
                nonDistinct        = source.nonDistinct;
                value              = source.value;
                startingValue      = source.startingValue;
            }
        };

        struct Debug_s
        {
            string text;
            string translation;
            int number;

            Debug_s()
                : number(-1)
            {}

            string toStr() const
            {
                return "@" + to_string(number) + " " + text;
            }

            string toStrShort() const
            {
                return "@" + to_string(number) + " " + trim(text, " \t");
            }
        };

        // structure fo final build
        struct Instruction_s
        {
            OpCode_e op;
            int64_t index;
            int64_t value;
            int64_t extra;
            Debug_s debug;

            Instruction_s(
                const OpCode_e op,
                const int64_t index,
                const int64_t value,
                const int64_t extra,
                Debug_s& dbg)
                : op(op),
                  index(index),
                  value(value),
                  extra(extra),
                  debug(dbg)
            {}

            Instruction_s(const OpCode_e op, const int64_t index, const int64_t value, const int64_t extra)
                : op(op),
                  index(index),
                  value(value),
                  extra(extra),
                  debug()
            {}
        };

        using InstructionList = vector<Instruction_s>;

        struct TextLiteral_s
        {
            int64_t hashValue; // xxhash of string
            int64_t index;
            string value;
        };

        using LiteralsList = vector<TextLiteral_s>;
        using VarList = vector<Variable_s>;
        using VarMap = unordered_map<string, Variable_s>;

        enum class sortOrder_e : int
        {
            ascending,
            descending
        };

        struct Function_s
        {
            string name;
            int64_t nameHash;
            int64_t execPtr;

            Function_s(const string& functionName, const int64_t codePtr)
                : name(functionName),
                  nameHash(MakeHash(functionName)),
                  execPtr(codePtr)
            {}
        };

        using FunctionList = vector<Function_s>;
        using ColumnLambdas = vector<int64_t>;

        struct Count_S
        {
            string name;
            int64_t functionHash;
        };

        enum class ColumnFilterOperator_e
        {
            eq,
            neq,
            gt,
            gte,
            lt,
            lte
        };

        struct Filter_s
        {
            int64_t personID {LLONG_MIN};
            bool cacheable {false};

            bool isRow {false};
            bool isEver {false};
            bool isLimit {false};
            bool isWithin {false};
            bool isRange {false};
            bool isFullSet {false};
            bool isReverse {false};
            bool isContinue {false};
            bool isNegated {false};
            bool isNext {false};
            bool isLookAhead {false}; // for within
            bool isLookBack {false}; // for within

            int evalBlock {-1};
            int continueBlock {-1};
            int withinStartBlock {-1};
            int withinWindowBlock {-1};
            int rangeStartBlock {-1};
            int rangeEndBlock {-1};

            int limitBlock {-1};
            int count {1};

            int comparator {-1};

            int64_t rangeStart {0};
            int64_t rangeEnd {LLONG_MAX};
            int64_t withinStart {0};
            int64_t withinWindow {LLONG_MAX};
            int64_t continueFrom {0};
        };

        using FilterList = vector<Filter_s>;
        using CountList = vector<Count_S>; // structure for variables
        using BlockMap = vector<int>;
        using AutoGrouping = vector<int>;

        struct Variables_S
        {
            VarList userVars;
            VarList tableVars;
            VarList columnVars;
            AutoGrouping autoGrouping;
            BlockMap blockList;
            ColumnLambdas columnLambdas;
            FunctionList functions;
            LiteralsList literals;
            CountList countList;
        };

        using HintPair = pair<string, HintOpList>;
        using HintPairs = vector<HintPair>;
        using ParamVars = unordered_map<string, cvar>;
        using SegmentList = vector<std::string>; // struct containing compiled macro
        using LambdaLookAside = vector<int>;
        using PropLookAside = vector<int>;

        struct Macro_s
        {
            Variables_S vars;
            LambdaLookAside lambdas;
            PropLookAside props;
            FilterList filters;
            InstructionList code;
            HintPairs indexes;
            std::string capturedIndex;
            std::string rawIndex;
            HintOpList index;
            bool indexIsCountable { false };
            string segmentName;
            SegmentList segments;
            MarshalSet marshalsReferenced;
            int64_t segmentTTL { -1 };
            int64_t segmentRefresh { -1 };
            int sessionColumn { -1 };

            int64_t sessionTime { 60'000LL * 30LL }; // 30 minutes
            std::string rawScript;
            bool isSegment { false };
            bool useProps { false };      // uses customer props
            bool writesProps { true };    // script can change props
            bool useGlobals { false };    // uses global for table
            bool useCached { false };     // for segments allow use of cached values within TTL
            bool isSegmentMath { false }; // for segments, the index has the value, script execution not required
            bool useSessions { false };   // uses session functions, we can cache these
            bool useStampedRowIds { false }; // count using row stamp rather than row uniqueness
            bool onInsert { false };
            int zIndex { 100 };
        };

        using QueryPairs = vector<pair<string, Macro_s>>;
    }
}
