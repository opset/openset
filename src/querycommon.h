#pragma once
#include "common.h"
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "dbtypes.h"
#include "attributes.h"
#include "var/var.h"
#include "../lib/str/strtools.h"

namespace openset
{
	namespace query
	{
		enum class blockType_e
		{
			code,
			lambda,
			function
		};

		// Result Column Modifiers						
		enum class modifiers_e : int32_t
		{
			sum,
			min,
			max,
			avg,
			count,
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
		};

		enum class opCode_e : int32_t
		{
			NOP = 0, // No operation

			PSHTBLCOL, // push column
			PSHRESCOL, // push result Column (may be grid, may be variable)
			//PSHRESGRP, // push group_by (may be grid, may be variable)
			VARIDX, // placeholder for an index to a variable
			PSHPAIR, // pushes a single pair dict to the stack
			PSHUSROBJ, // push object with deref
			PSHUSROREF, // push object with deref
			PSHUSRVAR, // push var
			PSHUSRVREF, // push pointer to variable (reference)
			PSHLITTRUE, // push True
			PSHLITFALSE, // push False
			PSHLITSTR, // push string
			PSHLITINT, // push integer
			PSHLITFLT, // push double
			PSHLITNUL, // push None value (NONE)

			POPUSROBJ, // pop object with deref
			POPUSRVAR, // pop variable
			POPTBLCOL, // pop column (place holder)
			POPRESGRP, // pop current group value
			POPRESCOL, // pop current result Column value

			CNDIF, // condition if
			CNDELIF, // condition else if
			CNDELSE, // condition else

			ITNEXT, // next iterator
			ITPREV, // prev iterator
			ITFOR, // for iterator

			MATHADD, // and last two stack items
			MATHSUB, // sub last two stack items
			MATHMUL, // multiply last two stack items
			MATHDIV, // divide last two stack items

			MATHADDEQ, // +=
			MATHSUBEQ, // -=
			MATHMULEQ, // *=
			MATHDIVEQ, // /=

			OPGT, // >
			OPLT, // <
			OPGTE, // >=
			OPLTE, // <-
			OPEQ, // == =
			OPNEQ, // != <>
			OPWTHN, // fuzzy range +/-
			OPNOT, // !

			LGCAND, // logical and
			LGCOR, // logical or

			MARSHAL, // Marshal an internal C++ function
			CALL, // Call a script defined function
			RETURN, // Pops the call stack leaves last item on stack

			TERM, // this script is done
			LGCNSTAND,
			LGCNSTOR
		};

		// Marshal Functions
		enum class marshals_e : int64_t
		{
			marshal_tally,
			marshal_now,
			marshal_event_time,
			marshal_last_event,
			marshal_first_event,
			marshal_prev_match,
			marshal_first_match,
			marshal_bucket,
			marshal_round,
			marshal_trunc,
			marshal_fix,
			marshal_to_seconds,
			marshal_to_minutes,
			marshal_to_hours,
			marshal_to_days,
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
			marshal_iter_get,
			marshal_iter_set,
			marshal_iter_move_first,
			marshal_iter_move_last,
			marshal_iter_next,
			marshal_iter_prev,
			marshal_event_count,
			marshal_iter_within,
			marshal_iter_between,
			marshal_population,
			marshal_intersection,
			marshal_union,
			marshal_compliment,
			marshal_difference,
			marshal_session,
			marshal_session_count,
			marshal_return,
			marshal_break,
			marshal_continue,
			marshal_log,
			marshal_emit,
			marshal_schedule,
			marshal_debug,
			marshal_exit,
			marshal_init_dict,
			marshal_init_list,
			marshal_make_dict,
			marshal_make_list,
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
			marshal_range, // not implemented
			marshal_str_split,
			marshal_str_find,
			marshal_str_rfind,
			marshal_str_replace,
			marshal_str_slice,
			marshal_str_strip,
			marshal_url_decode
		};

		// enum used for query index optimizer
		enum class hintOp_e : int64_t
		{
			UNSUPPORTED,
			PUSH_EQ,
			PUSH_NEQ,
			PUSH_GT,
			PUSH_GTE,
			PUSH_LT,
			PUSH_LTE,
			PUSH_PRESENT,
			PUSH_NOT,
			PUSH_NOP,
			BIT_OR,
			BIT_AND,
			NST_BIT_OR,
			NST_BIT_AND
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
	struct hash<openset::query::modifiers_e>
	{
		size_t operator()(const openset::query::modifiers_e& v) const
		{
			return static_cast<size_t>(v);
		}
	};

	template <>
	struct hash<openset::query::opCode_e>
	{
		size_t operator()(const openset::query::opCode_e& v) const
		{
			return static_cast<size_t>(v);
		}
	};

	template <>
	struct hash<openset::query::hintOp_e>
	{
		size_t operator()(const openset::query::hintOp_e& v) const
		{
			return static_cast<size_t>(v);
		}
	};

	template <>
	struct hash<openset::query::marshals_e>
	{
		size_t operator()(const openset::query::marshals_e& v) const
		{
			return static_cast<size_t>(v);
		}
	};
};

namespace openset
{
	namespace query
	{
		// String to Result Columns Modifier
		static const unordered_map<string, int64_t> timeConstants = {
					{"seconds", 1'000},
					{"second", 1'000},
					{"minute", 60'000},
					{"minutes", 60'000},
					{"hour", 3'600'000},
					{"hours", 3'600'000},
					{"day", 86'400'000},
					{"days", 86'400'000}
			};

		static const unordered_map<string, int64_t> withinConstants = {
					{"live", MakeHash("live")},
					{"first_event", MakeHash("first_event")},
					{"last_event", MakeHash("last_event")},
					{"prev_match", MakeHash("prev_match")},
					{"previous_match", MakeHash("previous_match")},
					{"first_match", MakeHash("first_match")},
			};

/*
		enum class within_e : int
		{
			live,
			first_event,
			last_event,
			prev_match,
			first_match
		};

		static const unordered_map<string, within_e> withinSwitchMap =
			{
					{"live", within_e::live},
					{"first_event", within_e::first_event},
					{"last_event", within_e::last_event},
					{"prev_match", within_e::prev_match},
					{"previous_match", within_e::prev_match},
					{"first_match", within_e::first_match},
			};
*/

		enum class timeSwitch_e : int
		{
			seconds,
			minutes,
			hours,
			days
		};

		static const unordered_map<string, timeSwitch_e> timeSwitchMap =
			{
					{"seconds", timeSwitch_e::seconds},
					{"minutes", timeSwitch_e::minutes},
					{"hours", timeSwitch_e::hours},
					{"days", timeSwitch_e::days}
			};

		// String to Result Columns Modifier
		static const unordered_map<string, modifiers_e> columnModifiers = {
					{"sum", modifiers_e::sum},
					{"min", modifiers_e::min},
					{"max", modifiers_e::max},
					{"avg", modifiers_e::avg},
					{"count", modifiers_e::count},
					{"value", modifiers_e::value},
					{"val", modifiers_e::value },
					{"variable", modifiers_e::var },
					{"var", modifiers_e::var},					
			};

		// Modifier to String (for debug output)
		static const unordered_map<modifiers_e, string> modifierDebugStrings = {
					{modifiers_e::sum, "SUM"},
					{modifiers_e::min, "MIN"},
					{modifiers_e::max, "MAX"},
					{modifiers_e::avg, "AVG"},
					{modifiers_e::count, "COUNT"},
					{modifiers_e::value, "VALUE"},
					{modifiers_e::var, "VAR"},
					{modifiers_e::second_number, "SECOND"},
					{modifiers_e::second_date, "DT_SECOND"},
					{modifiers_e::minute_number, "MINUTE"},
					{modifiers_e::minute_date, "DT_MINUTE"},
					{modifiers_e::hour_number, "HOUR"},
					{modifiers_e::hour_date, "DT_HOUR"},
					{modifiers_e::day_date, "DT_DAY"},
					{modifiers_e::day_of_week, "DAY_OF_WEEK"},
					{modifiers_e::day_of_month, "DAY_OF_MONTH"},
					{modifiers_e::day_of_year, "DAY_OF_YEAR"},
					{modifiers_e::week_date, "DT_WEEK"},
					{modifiers_e::month_date, "DT_MONTH"},
					{modifiers_e::month_number, "MONTH"},
					{modifiers_e::quarter_number, "QUARTER"},
					{modifiers_e::quarter_date, "DT_QUARTER"},
					{modifiers_e::year_number, "YEAR"},
					{modifiers_e::year_date, "DT_YEAR"},
			};

		// opCode to String (for debug output)
		static const unordered_map<opCode_e, string> opDebugStrings =
			{
					{opCode_e::NOP, "NOP"},

					{opCode_e::PSHTBLCOL, "PSHTBLCOL"},
					{opCode_e::PSHRESCOL, "PSHRESCOL"},
					{opCode_e::VARIDX, "VARIDX"},
					{opCode_e::PSHPAIR, "PSHPAIR"},
				//{opCode_e::PSHRESGRP, "PSHRESGRP"},
					{opCode_e::PSHUSROBJ, "PSHUSROBJ"},
					{opCode_e::PSHUSROREF, "PSHUSROREF" },
					{opCode_e::PSHUSRVAR, "PSHUSRVAR"},
					{ opCode_e::PSHUSRVREF, "PSHUSRVREF" },
					{opCode_e::PSHLITSTR, "PSHLITSTR"},
					{opCode_e::PSHLITINT, "PSHLITINT"},
					{opCode_e::PSHLITFLT, "PSHLITFLT"},
					{opCode_e::PSHLITNUL, "PSHLITNUL"},
					{ opCode_e::PSHLITTRUE, "PSHLITTRUE" },
					{ opCode_e::PSHLITFALSE, "PSHLITFALSE" },

					{opCode_e::POPUSROBJ, "POPUSROBJ"},
					{opCode_e::POPUSRVAR, "POPUSRVAR"},
					{opCode_e::POPTBLCOL, "POPTBLCOL"},
					{opCode_e::POPRESGRP, "POPRESGRP"},
					{opCode_e::POPRESCOL, "POPRESCOL"},

					{opCode_e::CNDIF, "CNDIF"},
					{opCode_e::CNDELIF, "CNDELIF"},
					{opCode_e::CNDELSE, "CNDELSE"},

					{opCode_e::ITNEXT, "ITNEXT"},
					{opCode_e::ITPREV, "ITPREV"},
					{opCode_e::ITFOR, "ITFOR"},

					{opCode_e::MATHADD, "MATHADD"},
					{opCode_e::MATHSUB, "MATHSUB"},
					{opCode_e::MATHMUL, "MATHMUL"},
					{opCode_e::MATHDIV, "MATHDIV"},

					{ opCode_e::MATHADDEQ, "OPADDEQ"},
					{ opCode_e::MATHSUBEQ, "OPSUBEQ"},
					{ opCode_e::MATHMULEQ, "OPMULEQ"},
					{ opCode_e::MATHDIVEQ, "OPDIVEQ"},

					{opCode_e::OPGT, "OPGT"},
					{opCode_e::OPLT, "OPLT"},
					{opCode_e::OPGTE, "OPGTE"},
					{opCode_e::OPLTE, "OPLTE"},
					{opCode_e::OPEQ, "OPEQ"},
					{opCode_e::OPNEQ, "OPNEQ"},
					{opCode_e::OPWTHN, "OPWTHN"},
					{opCode_e::OPNOT, "OPNOT"},

					{opCode_e::LGCAND, "LGCAND"},
					{opCode_e::LGCOR, "LGCOR"},

					{opCode_e::MARSHAL, "MARSHAL"},
					{opCode_e::CALL, "CALL"},
					{opCode_e::RETURN, "RETURN"},

					{opCode_e::TERM, "TERM"}

			};

		static const unordered_map<string, modifiers_e> timeModifiers =
			{
					{"second", modifiers_e::second_number},
					{"date_second", modifiers_e::second_number},
					{"minute", modifiers_e::minute_number},
					{"date_minute", modifiers_e::minute_date},
					{"hour", modifiers_e::hour_number},
					{"date_hour", modifiers_e::hour_date},
					{"date_day", modifiers_e::day_date},
					{"day_of_week", modifiers_e::day_of_week},
					{"day_of_month", modifiers_e::day_of_month},
					{"day_of_year", modifiers_e::day_of_year},
					{"date_week", modifiers_e::week_date},
					{"date_month", modifiers_e::month_date},
					{"month", modifiers_e::month_number},
					{"quarter", modifiers_e::quarter_number},
					{"date_quarter", modifiers_e::quarter_date},
					{"year", modifiers_e::year_number},
					{"date_year", modifiers_e::year_date},
			};

		static const unordered_set<modifiers_e> isTimeModifiers =
			{
			};

		static const unordered_set<string> redundantSugar =
			{
					{"of"},
					{"events"},
					{"event"}
			};

		// Marshal maps
		static const unordered_map<string, marshals_e> marshals =
			{
					{"tally", marshals_e::marshal_tally},
					{"now", marshals_e::marshal_now},
					{"event_time", marshals_e::marshal_event_time},
					{"last_event", marshals_e::marshal_last_event},
					{"first_event", marshals_e::marshal_first_event},
					{"prev_match", marshals_e::marshal_prev_match},
					{"first_match", marshals_e::marshal_first_match},
					{"bucket", marshals_e::marshal_bucket},
					{"round", marshals_e::marshal_round},
					{"trunc", marshals_e::marshal_trunc},
					{"fix", marshals_e::marshal_fix},
					{"to_seconds", marshals_e::marshal_to_seconds},
					{"to_minutes", marshals_e::marshal_to_minutes},
					{"to_hours", marshals_e::marshal_to_hours},
					{"to_days", marshals_e::marshal_to_days},
					{"get_second", marshals_e::marshal_get_second},
					{"date_second", marshals_e::marshal_round_second},
					{"get_minute", marshals_e::marshal_get_minute},
					{"date_minute", marshals_e::marshal_round_minute},
					{"get_hour", marshals_e::marshal_get_hour},
					{"date_hour", marshals_e::marshal_round_hour},
					{"date_day", marshals_e::marshal_round_day},
					{"get_day_of_week", marshals_e::marshal_get_day_of_week},
					{"get_day_of_month", marshals_e::marshal_get_day_of_month},
					{"get_day_of_year", marshals_e::marshal_get_day_of_year},
					{"date_week", marshals_e::marshal_round_week},
					{"date_month", marshals_e::marshal_round_month},
					{"get_month", marshals_e::marshal_get_month},
					{"get_quarter", marshals_e::marshal_get_quarter},
					{"date_quarter", marshals_e::marshal_round_quarter},
					{"get_year", marshals_e::marshal_get_year},
					{"date_year", marshals_e::marshal_round_year},
					{"emit", marshals_e::marshal_emit},
					{"schedule", marshals_e::marshal_schedule},
					{"iter_get", marshals_e::marshal_iter_get },
					{"iter_set", marshals_e::marshal_iter_set },
					{"iter_move_first", marshals_e::marshal_iter_move_first },
					{"iter_move_last", marshals_e::marshal_iter_move_last },
					{"iter_next", marshals_e::marshal_iter_next },
					{"iter_prev", marshals_e::marshal_iter_prev },
					{"event_count", marshals_e::marshal_event_count },
					{"iter_within", marshals_e::marshal_iter_within},
					{"iter_between", marshals_e::marshal_iter_between},
					{"population", marshals_e::marshal_population },
					{"intersection", marshals_e::marshal_intersection},
					{"union", marshals_e::marshal_union},
					{"compliment", marshals_e::marshal_compliment},
					{"difference", marshals_e::marshal_difference},
					{"marshal_session", marshals_e::marshal_session},
					{"marshal_session_count", marshals_e::marshal_session_count},
					{"return", marshals_e::marshal_return},
					{"continue", marshals_e::marshal_continue},
					{"break", marshals_e::marshal_break},
					{"log", marshals_e::marshal_log},
					{"debug", marshals_e::marshal_debug},
					{"exit", marshals_e::marshal_exit},
					{"__internal_init_dict", marshals_e::marshal_init_dict},
					{"__internal_init_list", marshals_e::marshal_init_list},
					{ "set", marshals_e::marshal_set },
					{ "list", marshals_e::marshal_list },
					{ "dict", marshals_e::marshal_dict },
					{ "int", marshals_e::marshal_int },
					{ "float", marshals_e::marshal_float },
					{ "str", marshals_e::marshal_str },
					{"__internal_make_dict", marshals_e::marshal_make_dict},
					{"__internal_make_list", marshals_e::marshal_make_list},
					{ "len", marshals_e::marshal_len },
					{ "__append", marshals_e::marshal_append },
					{ "__update", marshals_e::marshal_update },
					{ "__add", marshals_e::marshal_add },
					{ "__remove", marshals_e::marshal_remove },
					{ "__del", marshals_e::marshal_del },
					{ "__contains", marshals_e::marshal_contains },
					{ "__notcontains", marshals_e::marshal_not_contains },
					{ "__pop", marshals_e::marshal_pop },
					{ "__clear", marshals_e::marshal_clear },
					{ "__keys", marshals_e::marshal_keys },
					{ "__split", marshals_e::marshal_str_split },
					{ "__find", marshals_e::marshal_str_find },
					{ "__rfind", marshals_e::marshal_str_rfind },
					{ "__slice", marshals_e::marshal_str_slice },
					{ "__strip", marshals_e::marshal_str_strip},
					{ "range", marshals_e::marshal_range },
					{ "url_decode", marshals_e::marshal_url_decode }
			};

		static const unordered_set<marshals_e> segmentMathMarshals =
			{
					{marshals_e::marshal_population },
					{marshals_e::marshal_intersection},
					{marshals_e::marshal_union},
					{marshals_e::marshal_compliment},
					{marshals_e::marshal_difference},
			};

		static const unordered_set<marshals_e> sessionMarshals =
			{
					{marshals_e::marshal_session},
					{marshals_e::marshal_session_count},
			};

		// these are marshals that do not take params by default, so they appear
		// like variables.
		static const unordered_set<string> macroMarshals =
			{
					{"now"},
					{"event_time"},
					{"last_event"},
					{"first_event"},
					{"prev_match"},
					{"first_match"},
					{"session_count"},
					{"session"},
					{"__internal_init_dict"},
					{"__internal_init_list"},
			};

		// Comparatives
		static const unordered_map<string, opCode_e> operators = {
					{">=",opCode_e::OPGTE},
					{"<=",opCode_e::OPLTE},
					{">",opCode_e::OPGT},
					{"<",opCode_e::OPLT},
					{"==",opCode_e::OPEQ},
					{"is",opCode_e::OPEQ},
					{ "=",opCode_e::OPEQ },
					{"!=",opCode_e::OPNEQ},
					{"<>",opCode_e::OPNEQ},
					{"not",opCode_e::OPNOT},
					{"isnot",opCode_e::OPNEQ},
					// {"within",opCode_e::OPWTHN},
			};

		static const unordered_map<string, opCode_e> mathAssignmentOperators = {
			{ "+=", opCode_e::MATHADDEQ }, // math operators
			{ "-=", opCode_e::MATHSUBEQ },
			{ "*=", opCode_e::MATHMULEQ },
			{ "/=", opCode_e::MATHDIVEQ },
		};

		static const unordered_map<opCode_e, string> operatorsDebug = {
					{opCode_e::OPGTE, ">="},
					{opCode_e::OPLTE, "<="},
					{opCode_e::OPGT, ">"},
					{opCode_e::OPLT, "<"},
					{opCode_e::OPEQ, "=="},
					{opCode_e::OPNEQ, "!="},
					{opCode_e::OPNOT, "!"}
					// {opCode_e::OPWTHN, "within"}
			};

		// Math
		static const unordered_map<string, opCode_e> math =
		{
				{"+", opCode_e::MATHADD},
				{"-", opCode_e::MATHSUB},
				{"*", opCode_e::MATHMUL},
				{"/", opCode_e::MATHDIV}
		};

		// Conditionals
		static const unordered_map<string, opCode_e> logicalOperators =
			{
					{"and", opCode_e::LGCAND},
					{"or", opCode_e::LGCOR},
					{"in", opCode_e::LGCOR},
					{"nest_and", opCode_e::LGCNSTAND},
					{"nest_or", opCode_e::LGCNSTOR},
			};

		static const unordered_map<opCode_e, string> logicalOperatorsDebug = {
					{opCode_e::LGCAND, "and"},
					{opCode_e::LGCOR, "or"},
			};

		static const unordered_map<hintOp_e, string> hintOperatorsDebug = {
					{hintOp_e::UNSUPPORTED, "UNSUP"},
					{hintOp_e::PUSH_EQ, "PUSH_EQ"},
					{hintOp_e::PUSH_NEQ, "PUSH_NEQ"},
					{hintOp_e::PUSH_GT, "PUSH_GT"},
					{hintOp_e::PUSH_GTE, "PUSH_GTE"},
					{hintOp_e::PUSH_LT, "PUSH_LT"},
					{hintOp_e::PUSH_LTE, "PUSH_LTE"},
					{hintOp_e::PUSH_PRESENT, "PUSH_PRES"},
					{hintOp_e::PUSH_NOP, "PUSH_NOP"},
					{hintOp_e::BIT_OR, "BIT_OR"},
					{hintOp_e::BIT_AND, "BIT_AND"},
					{hintOp_e::NST_BIT_OR, "NST_BIT_OR"},
					{hintOp_e::NST_BIT_AND, "NST_BIT_AND"},

			};

		static const unordered_map<opCode_e, hintOp_e> opToHintOp = {
					{opCode_e::OPGTE, hintOp_e::PUSH_GTE},
					{opCode_e::OPLTE, hintOp_e::PUSH_LTE},
					{opCode_e::OPGT, hintOp_e::PUSH_GT},
					{opCode_e::OPLT, hintOp_e::PUSH_LT},
					{opCode_e::OPEQ, hintOp_e::PUSH_EQ},
					{opCode_e::OPNEQ, hintOp_e::PUSH_NEQ},
					{opCode_e::OPNOT, hintOp_e::PUSH_NOT},
					{opCode_e::LGCAND, hintOp_e::BIT_AND},
					{opCode_e::LGCOR, hintOp_e::BIT_OR},
					{opCode_e::LGCNSTOR, hintOp_e::NST_BIT_OR},
					{opCode_e::LGCNSTAND, hintOp_e::NST_BIT_AND},
			};

		struct hintOp_s
		{
			hintOp_e op;
			string column;
			int64_t intValue;
			string textValue;
			bool numeric;

			hintOp_s(const hintOp_e op, 
				     const string column, 
				     const int64_t intValue) :
				op(op),
				column(column),
				intValue(intValue),
				numeric(true)
			{}

			hintOp_s(const hintOp_e op, 
				     const string column, 
				     const string text) :
				op(op),
				column(column),
				intValue(0),
				numeric(false)
			{
				if (text == "None") // special case
				{
					intValue = NONE;
					numeric = true;
				}
				else
				{
					textValue = text.substr(1, text.length() - 2);
					intValue = MakeHash(textValue);
				}
			}

			explicit hintOp_s(const hintOp_e op) :
				op(op),
				intValue(0),
				numeric(false)
			{}
		};

		using HintOpList = vector<hintOp_s>;

		struct Variable_s
		{
			string actual; // actual name
			string alias; // alias
			string space; // namespace
			string distinctColumnName{"__action"}; // name of column used for aggregators
			modifiers_e modifier{modifiers_e::value}; // default is value
			int index{-1}; // index
			int column{-1}; // column in grid
			int schemaColumn{-1}; // column in schema
			int distinctColumn{openset::db::COL_ACTION}; // column containing distinct key
			db::columnTypes_e schemaType{db::columnTypes_e::freeColumn};
			int popRefs{0}; // reference counter for pops
			int pushRefs{0}; // reference counter for pushes
			int sortOrder{-1}; // used for sorting in column order
			int lambdaIndex{-1}; // used for variable assignment by lambada
			bool nonDistinct{ false };

			cvar value{NONE};
			cvar startingValue{NONE};

			Variable_s()
			{}

			Variable_s(const string actual, 
					   const string space, 
				       const int sortOrder = -1):
				actual(actual),
				alias(actual),
				space(space),
				sortOrder(sortOrder)
			{}

			Variable_s(const string actual, 
					   const string alias,
			           const string space,
			           const modifiers_e modifier = modifiers_e::value,
			           const int sortOrder = -1):
				actual(actual),
				alias(alias),
				space(space),
				modifier(modifier),
				sortOrder(sortOrder)
			{}

			Variable_s(const Variable_s& source)
			{
				actual = source.actual;
				alias = source.alias;
				space = source.space;
				distinctColumnName = source.distinctColumnName;
				distinctColumn = source.distinctColumn;
				modifier = source.modifier;
				index = source.index;
				column = source.column;
				schemaColumn = source.schemaColumn;

				schemaType = source.schemaType;
				popRefs = source.popRefs;
				pushRefs = source.pushRefs;
				sortOrder = source.sortOrder;
				lambdaIndex = source.lambdaIndex;

				nonDistinct = source.nonDistinct;

				value = source.value;
				startingValue = source.startingValue;
			}
		};

		struct Debug_s
		{
			string text;
			string translation;
			int number;

			Debug_s() :
				number(-1)
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
			opCode_e op;
			int64_t index;
			int64_t value;
			int64_t extra;
			Debug_s debug;

			Instruction_s(
				const opCode_e op,
				const int64_t index,
				const int64_t value,
				const int64_t extra,
				Debug_s& dbg) :
				op(op),
				index(index),
				value(value),
				extra(extra),
				debug(dbg)
			{}

			Instruction_s(
				const opCode_e op,
				const int64_t index,
				const int64_t value,
				const int64_t extra) :
				op(op),
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

		struct Sort_s
		{
			string name;
			sortOrder_e order;
			int64_t column;

			Sort_s(const string columnName, const sortOrder_e sortOrder):
				name(columnName),
				order(sortOrder),
				column(-1)
			{}
		};

		using SortList = vector<Sort_s>;

		struct Function_s
		{
			string name;
			int64_t nameHash;
			int64_t execPtr;

			Function_s(const string functionName, const int64_t codePtr):
				name(functionName),
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

		using CountList = vector<Count_S>;

		// structure for variables
		struct Variables_S
		{
			VarList userVars;
			VarList tableVars;
			VarList columnVars;
			ColumnLambdas columnLambdas;
			SortList sortOrder;
			FunctionList functions;
			LiteralsList literals;
			CountList countList;
		};

		using HintPair = pair<string, HintOpList>;
		using HintPairs = vector<HintPair>;
		using ParamVars = unordered_map<string, cvar>;
		using SegmentList = vector<std::string>;

		// struct containing compiled macro
		struct Macro_S
		{
			Variables_S vars;
			InstructionList code;
			HintPairs indexes;
			bool isSegment;
			string segmentName;
			int64_t segmentTTL;
			int64_t segmentRefresh;
			SegmentList segments;

			bool useGlobals; // uses global for table
			bool useCached; // for segments allow use of cached values within TTL
			bool isSegmentMath; // for segments, the index has the value, script execution not required
			bool useSessions; // uses session functions, we can cache these

			Macro_S() :
				isSegment(false),
				segmentTTL(-1),
				segmentRefresh(-1),
				useGlobals(false),
				useCached(false),
				isSegmentMath(false),
				useSessions(false)
			{};
		};

		using QueryPairs = vector<pair<string, Macro_S>>;
	}
}
