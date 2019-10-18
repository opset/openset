#include "queryparserosl.h"

#include "properties.h"
#include <iterator>
#include <sstream>
#include <iomanip>

string padding(string text, const int length, const bool left = true, const char filler = ' ')
{
    while (static_cast<int>(text.length()) < length)
        text = (left) ? filler + text : text + filler;
    return text;
}

string padding(const int64_t number, const int length, const bool left = true, const char filler = ' ')
{
    return padding(to_string(number), length, left, filler);
}

string openset::query::MacroDbg(Macro_s& macro)
{
    stringstream ss;
    const auto outSpacer = [&ss]()
    {
        ss <<
            "--------------------------------------------------------------------------------------------------------------------------------------------------------"
            << endl;
    };
    ss << endl;
    ss << "Raw Script:" << endl;
    outSpacer();
    ss << macro.rawScript << endl;
    outSpacer();
    ss << endl << endl;
    ss << "Text literals:" << endl;
    outSpacer();
    ss << "IDX | ID               | TEXT + HEX" << endl;
    outSpacer();
    if (macro.vars.literals.size())
    {
        for (auto& v : macro.vars.literals)
        {
            ss << padding(v.index, 3, true) << " | ";
            ss << "#" << hex << v.hashValue << " | ";
            ss << "\"" << v.value << "\" hex: ";
            for (auto ch : v.value)
                ss << setfill('0') << setw(2) << hex << abs(cast<int>(ch)) << ' ';
            ss << endl;
        } // stop hexing!
        cout << dec;
    }
    else
        ss << "NONE" << endl;
    outSpacer();
    ss << endl << endl;
    ss << "User variables:" << endl;
    outSpacer();
    ss << "IDX | NAME                   | PROP" << endl;
    outSpacer();
    if (macro.vars.userVars.size())
    {
        for (auto& v : macro.vars.userVars)
        {
            ss << padding(v.index, 3, true) << " | ";
            ss << padding("'" + v.actual + "'", 20, false, ' ') << " | " <<
                (v.isProp ? "is property" : "");
            ss << endl;
        }
    }
    else
        ss << "NONE" << endl;
    outSpacer();
    ss << endl << endl;
    ss << "Table Properties Map (in script or aggregates):" << endl;
    outSpacer();
    ss << "IDX | PRPIDX | NAME                 | TYPE      | NOTE" << endl;
    outSpacer();
    if (macro.vars.tableVars.size())
    {
        for (auto& v : macro.vars.tableVars)
        {
            ss << padding(v.index, 3, true) << " | ";
            ss << padding(v.schemaColumn, 6, true) << " | ";
            ss << padding(v.actual, 20, false) << " | ";
            std::string type;
            switch (v.schemaType)
            {
            case db::PropertyTypes_e::freeProp:
                type = "err(1)";
                break;
            case db::PropertyTypes_e::intProp:
                type = "int";
                break;
            case db::PropertyTypes_e::doubleProp:
                type = "double";
                break;
            case db::PropertyTypes_e::boolProp:
                type = "bool";
                break;
            case db::PropertyTypes_e::textProp:
                type = "text";
                break;
            default:
                type = "err(2)";
            }
            ss << padding(type, 9, false) << " | "; /*
            if (v.actual == "__uuid")
                ss << "actual for 'customer' or 'people'";
            else if (v.actual == "__action")
                ss << "actual for 'action'";
            else if (v.actual == "__stamp")
                ss << "actual for 'stamp'";
            else if (v.actual == "__session")
                ss << "actual for 'session'";
            */
            ss << endl;
        }
    }
    else
        ss << "NONE" << endl;
    outSpacer();
    ss << endl << endl;
    ss << "Aggregates:" << endl;
    outSpacer();
    ss << "AGGIDX | TBLIDX | AGG    | NAME                 | ALIAS                | NOTE" << endl;
    outSpacer();
    if (macro.vars.columnVars.size())
    {
        for (auto& v : macro.vars.columnVars)
        {
            ss << padding(v.index, 6, true) << " | ";
            ss << padding(v.column, 6, true) << " | ";
            ss << padding(ModifierDebugStrings.find(v.modifier)->second, 6, false) << " | ";
            if (v.column == -1)
                ss << "  NA  | ";
            else
                ss << padding(v.actual, 20, false) << " | ";
            ss << padding(v.alias, 20, false) << " | "; /*
            if (v.actual == "__uuid")
                ss << "from 'customer' or 'people'  ";
            else if (v.actual == "__action")
                ss << "from 'action'  ";
            else if (v.actual == "__stamp")
                ss << "from 'stamp'  ";
            else if (v.actual == "__session")
                ss << "from 'session'  ";
            */
            if (v.distinctColumnName != v.actual)
                ss << "distinct: " << v.distinctColumnName;
            ss << endl;
        }
    }
    else
        ss << "NONE" << endl;
    outSpacer();
    ss << endl << endl;
    ss << "PyQL Marshals:" << endl;
    outSpacer();
    ss << "FUNC# | MARSHAL" << endl;
    outSpacer();
    if (macro.marshalsReferenced.size())
    {
        const auto getMarshalName = [](const Marshals_e marshalCode)-> std::string
        {
            for (auto& m : Marshals)
                if (m.second == marshalCode)
                    return m.first;
            return "__MISSING__";
        };
        for (auto& m : macro.marshalsReferenced)
        {
            ss << padding(static_cast<int64_t>(m), 5, true) << " | ";
            ss << getMarshalName(m) << endl;
        }
    }
    else
        ss << "NONE" << endl;
    outSpacer();
    ss << endl << endl;
    ss << "User Functions:" << endl;
    outSpacer();
    ss << " OFS | NAME" << endl;
    outSpacer();
    if (macro.vars.functions.size())
    {
        for (auto& f : macro.vars.functions)
        {
            ss << padding(f.execPtr, 4, true, '0') << " | ";
            ss << f.name;
            ss << endl;
        }
        ss << endl;
    }
    else
        ss << "NONE" << endl;

    outSpacer();
    ss << endl << endl;
    ss << "Raw Derived Index (all index conditions are 'ever'):" << endl;
    outSpacer();
    ss << "Captured Logic:" << endl;
    ss << macro.capturedIndex << endl;
    ss << "Reduced Logic:" << endl;
    ss << macro.rawIndex << endl;
    outSpacer();
    ss << endl;
    ss << "Index Macros:" << endl;
    outSpacer();
    ss << "OP             | VALUE" << endl;
    outSpacer();
    for (auto& i : macro.index)
    {
        const auto op = HintOperatorsDebug.find(i.op)->second;
        ss << padding(op, 14, false) << " | ";
        switch (i.op)
        {
        case HintOp_e::PUSH_TBL:
            ss << "@" << padding(i.value.getString(), 20, false);
            break;
        case HintOp_e::PUSH_VAL:
            ss << padding(i.value.getString(), 20, false);
            break;
        }
        ss << endl;
    }

    ss << endl << endl;
    ss << "Assembly:" << endl;
    outSpacer();
    ss << "OFS  | OP           |           VAL |      IDX |      EXT | LINE | CODE" << endl;
    outSpacer();
    auto count = 0;
    for (auto& m : macro.code)
    {
        const auto opString = OpDebugStrings.find(m.op)->second;
        ss << padding(count, 4, true, '0') << " | ";
        ss << padding(opString, 12, false) << " | ";
        ss << (m.value == 9999999
                   ? padding("INF", 13)
                   : padding(m.value, 13)) << " | ";
        ss << padding(m.index, 8) << " | ";
        ss << (m.extra == NONE
                   ? "       -"
                   : padding(m.extra, 8)) << " | ";
        ss << ((m.debug.number == -1)
                   ? "    "
                   : padding("#" + to_string(m.debug.number), 4)) << " | ";
        ss << m.debug.text;
        ss << endl;
        if (m.debug.translation.length())
        {
            std::string spaces = "";
            auto it            = m.debug.text.begin();
            while (*it == ' ')
            {
                spaces += ' ';
                ++it;
            }
            ss << "     |              |               |          |          | ";
            ss << "   > | " << spaces << m.debug.translation << endl;
        }
        ++count;
    }
    outSpacer();
    return ss.str();
}
