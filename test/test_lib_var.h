#pragma once

#include "common.h"
#include "testing.h"
#include "../lib/var/var.h"
#include "../lib/var/varblob.h"
#include "../lib/heapstack/heapstack.h"

// Our tests
inline Tests test_lib_cvar()
{
    return {
            {
                "cvar: int32_t on create", []
                {
                    cvar somecvar = 1234;
                    ASSERT(somecvar == 1234);
                    ASSERT(somecvar.typeOf() == cvar::valueType::INT32);
                }
            },
            {
                "cvar: int64_t on create", []
                {
                    cvar somecvar = int64_t(1234);
                    ASSERT(somecvar == int64_t(1234));
                    ASSERT(somecvar.typeOf() == cvar::valueType::INT64);
                }
            },
            {
                "cvar: float on create", []
                {
                    cvar somecvar = 1234.5f;
                    ASSERT(somecvar == 1234.5f);
                    ASSERT(somecvar.typeOf() == cvar::valueType::FLT);
                }
            },
            {
                "cvar: double on create", []
                {
                    cvar somecvar = 1234.5f;
                    ASSERT(somecvar == 1234.5f);
                    ASSERT(somecvar.typeOf() == cvar::valueType::FLT);
                }
            },
            {
                "cvar: char* on create", []
                {
                    const char someCharStar[] = "I'm old school but fast";
                    cvar somecvar = someCharStar;
                    ASSERT(somecvar == "I'm old school but fast");
                    ASSERT(somecvar.typeOf() == cvar::valueType::STR);
                }
            },
            {
                "cvar: std::string on create", []
                {
                    const char someCharStar[] = "I'm modern";
                    cvar somecvar = someCharStar;
                    ASSERT(somecvar == "I'm modern");
                    ASSERT(somecvar.typeOf() == cvar::valueType::STR);
                }
            },
            {
                "cvar: bool on create", []
                {
                    cvar somecvar = true;
                    ASSERT(somecvar == true);
                    ASSERT(somecvar.typeOf() == cvar::valueType::BOOL);
                }
            },
            {
                "cvar: string numeric to all types", []
                {
                    cvar somecvar = "1234.5";

                    int someInt = somecvar;
                    ASSERT(someInt == 1234);

                    int64_t someInt64 = somecvar;
                    ASSERT(someInt64 == 1234);

                    float someFloat = somecvar;
                    ASSERT(someFloat == 1234.5);

                    double someDouble = somecvar;
                    ASSERT(someDouble == 1234.5);

                    bool someBool = somecvar;
                    ASSERT(someBool == true);
                }
            },
            {
                "cvar: unary operator on all types", []
                {
                    cvar somecvar;

                    somecvar = "1234.5";
                    somecvar = -somecvar;
                    ASSERT(somecvar == "-1234.5");

                    somecvar = 1234.5;
                    somecvar = -somecvar;
                    ASSERT(somecvar == -1234.5);

                    somecvar = 1234.5f;
                    somecvar = -somecvar;
                    ASSERT(somecvar == -1234.5f);

                    somecvar = 1234;
                    somecvar = -somecvar;
                    ASSERT(somecvar == -1234);

                    somecvar = int64_t(1234);
                    somecvar = -somecvar;
                    ASSERT(somecvar == int64_t(-1234));
                }
            },
            {
                "cvar: string math operators", []
                {
                    cvar somecvar = "the rain in Spain";

                    somecvar -= "Spain";
                    ASSERT(somecvar == "the rain in ");

                    somecvar += "Canada";
                    ASSERT(somecvar == "the rain in Canada");

                    somecvar = somecvar + " is wet";
                    ASSERT(somecvar == "the rain in Canada is wet");

                    somecvar = somecvar - " is wet";
                    ASSERT(somecvar == "the rain in Canada");
                }
            },
            {
                "cvar: text assignment with text addition", []
                {
                    cvar somecvar = 49.5f + 5;
                    ASSERT(somecvar == 54.5);

                    somecvar = "49.5"_cvar + 5;
                    ASSERT(somecvar == "49.55");

                    somecvar = "49.5"_cvar + "5"_cvar;
                    ASSERT(somecvar == "49.55");

                    somecvar = "49.5"s + "5"s;
                    ASSERT(somecvar == "49.55");

                    somecvar = 5 + "49.5"_cvar;
                    ASSERT(somecvar == "549.5");

                    somecvar = "5"_cvar + "49.5"_cvar;
                    ASSERT(somecvar == "549.5");

                    somecvar = "5"s + "49.5"s;
                    ASSERT(somecvar == "549.5");
                }
            },
            {
                "cvar: numeric assignment with numeric math operators", []
                {
                    cvar somecvar = 49.5f + 5;
                    ASSERT(somecvar == 54.5);

                    somecvar = 49.5f + 5.5f - 2;
                    ASSERT(somecvar.typeOf() == cvar::valueType::FLT);
                    ASSERT(somecvar == 53.0f);
                }
            },
            {
                "cvar: user defined literals {value}_cvar on types", []
                {
                    cvar somecvar = 49.5_cvar;
                    ASSERT(somecvar.typeOf() == cvar::valueType::DBL);
                    ASSERT(somecvar == 49.5);

                    somecvar = -49.5_cvar;
                    ASSERT(somecvar.typeOf() == cvar::valueType::DBL);
                    ASSERT(somecvar == -49.5);

                    somecvar = 50_cvar;
                    ASSERT(somecvar.typeOf() == cvar::valueType::INT64);
                    ASSERT(somecvar == 50);

                    somecvar = -50_cvar; // literal overload takes unsigned test unary
                    ASSERT(somecvar.typeOf() == cvar::valueType::INT64);
                    ASSERT(somecvar == -50);

                    somecvar = "what goes up"_cvar; // literal overload takes unsigned test unary
                    ASSERT(somecvar.typeOf() == cvar::valueType::STR);
                    ASSERT(somecvar == "what goes up");
                }
            },
            {
                "cvar: test JS like conversion functions", []
                {
                    cvar somecvar, other;

                    somecvar = "1234.5";
                    other = float(somecvar);
                    ASSERT(other.typeOf() == cvar::valueType::FLT);
                    ASSERT(other == 1234.5f);

                    somecvar = 1234.5; // double by default
                    other = double(somecvar);
                    ASSERT(other.typeOf() == cvar::valueType::DBL);
                    ASSERT(other == 1234.5);

                    somecvar = "1234.5";
                    other = int(somecvar);
                    ASSERT(other.typeOf() == cvar::valueType::INT32);
                    ASSERT(other == 1234);

                    somecvar = "1234.5";
                    other = int64_t(somecvar);
                    ASSERT(other.typeOf() == cvar::valueType::INT64);
                    ASSERT(other == int64_t(1234));

                    somecvar = 1234.5;
                    other = std::string(somecvar);
                    ASSERT(other.typeOf() == cvar::valueType::STR);
                    ASSERT(other == "1234.5");

                    somecvar = 1234.5;
                    other = somecvar.getString(); // manually
                    ASSERT(other.typeOf() == cvar::valueType::STR);
                    ASSERT(other == "1234.5");
                }
            },
            {
                "cvar: cvar-to-cvar cross type comparison operators", []
                {
                    cvar left, right;

                    left = "123";
                    right = 123;
                    ASSERT(left.typeOf() == cvar::valueType::STR);
                    ASSERT(right.typeOf() == cvar::valueType::INT32);
                    ASSERT(left == right);

                    left = "123.5";
                    right = 123.5;
                    ASSERT(left.typeOf() == cvar::valueType::STR);
                    ASSERT(right.typeOf() == cvar::valueType::DBL);
                    ASSERT(left == right);

                    left = 123.5f;
                    right = 123.5;
                    ASSERT(left.typeOf() == cvar::valueType::FLT);
                    ASSERT(right.typeOf() == cvar::valueType::DBL);
                    ASSERT(left == right);

                    left = 123;
                    right = int64_t(123);
                    ASSERT(left.typeOf() == cvar::valueType::INT32);
                    ASSERT(right.typeOf() == cvar::valueType::INT64);
                    ASSERT(left == right);

                    left = "1";
                    right = true;
                    ASSERT(left.typeOf() == cvar::valueType::STR);
                    ASSERT(right.typeOf() == cvar::valueType::BOOL);
                    ASSERT(left == right);

                    left = "true";
                    right = true;
                    ASSERT(left.typeOf() == cvar::valueType::STR);
                    ASSERT(right.typeOf() == cvar::valueType::BOOL);
                    ASSERT(left == right);

                    left = "true";
                    right = false;
                    ASSERT(left.typeOf() == cvar::valueType::STR);
                    ASSERT(right.typeOf() == cvar::valueType::BOOL);
                    ASSERT(left != right);

                    left = "";
                    right = false;
                    ASSERT(left.typeOf() == cvar::valueType::STR);
                    ASSERT(right.typeOf() == cvar::valueType::BOOL);
                    ASSERT(left == right);
                }
            },
            {
                "cvar: cvar-to-POD cross type comparison operators", []
                {
                    cvar somecvar;

                    somecvar = "123";
                    ASSERT(somecvar == 123); // compare cvar left and cvar right overloads
                    ASSERT(123 == somecvar);

                    somecvar = "123.5";
                    ASSERT(somecvar == 123.5);
                    ASSERT(123.5 == somecvar);
                    ASSERT(somecvar == 123.5f);
                    ASSERT(123.5f == somecvar);

                    somecvar = "123.0";
                    ASSERT(somecvar == 123.0);
                    ASSERT(123.0 == somecvar);
                    ASSERT(somecvar == 123.0f);
                    ASSERT(123.0f == somecvar);

                    somecvar = "123";
                    ASSERT(somecvar == 123.0);
                    ASSERT(123.0 == somecvar);
                    ASSERT(somecvar == 123.0f);
                    ASSERT(123.0f == somecvar);

                    somecvar = 123.5;
                    ASSERT(somecvar == 123.5f);
                    ASSERT(123.5f == somecvar);

                    somecvar = 123.5f;
                    ASSERT(somecvar == 123.5);
                    ASSERT(123.5 == somecvar);

                    somecvar = 123;
                    ASSERT(somecvar == int64_t(123));
                    ASSERT(int64_t(123) == somecvar);

                    somecvar = int64_t(123);
                    ASSERT(somecvar == 123);
                    ASSERT(123 == somecvar);

                    somecvar = "true";
                    ASSERT(somecvar == true);
                    ASSERT(true == somecvar);

                    somecvar = "1";
                    ASSERT(somecvar == true);
                    ASSERT(true == somecvar);

                    somecvar = true;
                    ASSERT(somecvar == "true");
                    ASSERT("true" == somecvar);

                    somecvar = true;
                    ASSERT(somecvar == "1");
                    ASSERT("1" == somecvar);

                    somecvar = "false";
                    ASSERT(somecvar == false);
                    ASSERT(false == somecvar);

                    somecvar = "0";
                    ASSERT(somecvar == false);
                    ASSERT(false == somecvar);

                    somecvar = false;
                    ASSERT(somecvar == "false");
                    ASSERT("false" == somecvar);

                    somecvar = false;
                    ASSERT(somecvar == "0");
                    ASSERT("0" == somecvar);
                }
            },
            {
                "cvar: container constructors", []
                {
                    cvar somecvar;

                    somecvar.set(); // this is now a set
                    ASSERT(somecvar.typeOf() == cvar::valueType::SET);

                    somecvar.list(); // this is now a list
                    ASSERT(somecvar.typeOf() == cvar::valueType::LIST);

                    somecvar.dict(); // this is now a dict
                    ASSERT(somecvar.typeOf() == cvar::valueType::DICT);

                    // assignment direct from unordered_set
                    somecvar = std::unordered_set<int64_t>{1, 2, 3, 4, 5, 1};
                    ASSERT(somecvar.typeOf() == cvar::valueType::SET);
                    ASSERT(somecvar.len() == 5);
                    ASSERT(somecvar.contains(3));

                    // direct from vector
                    somecvar = std::vector<int64_t>{1, 2, 3, 4, 5, 1};
                    ASSERT(somecvar.typeOf() == cvar::valueType::LIST);
                    ASSERT(somecvar.len() == 6);

                    cvar someList = cvar::l{1, 2, 3, 4, 5, 1};
                    ASSERT(someList.typeOf() == cvar::valueType::LIST);
                    ASSERT(someList.len() == 6);

                    // direct from unordered_map
                    somecvar = std::unordered_map<std::string, std::string>{
                            {"tree", "house"},
                            {"big", "thinking"},
                            {"salt", "water"}
                    };
                    ASSERT(somecvar.typeOf() == cvar::valueType::DICT);
                    ASSERT(somecvar.len() == 3);
                    ASSERT(somecvar["salt"] == "water");

                    // direct from pair
                    somecvar = std::pair<cvar, cvar>{1234, "is a number"};
                    ASSERT(somecvar.typeOf() == cvar::valueType::DICT);
                    ASSERT(somecvar.len() == 1);
                    ASSERT(somecvar[1234] == "is a number");

                    // this is pretty nifty... almost looks like JSON.

                    /* JSON:
                     * {
                     *     "things": {
                     *         "hello": "goodbye",
                     *         "tea": "biscuit"
                     *     },
                     *     "this": [1,2,"teeth",4],
                     *     "feet": "mouth"
                     * }
                     */

                    cvar anothercvar = {
                            {
                                "things", {
                                        {"hello"s, "goodbye"}, // mix some strings
                                    cvar::o{"tea", "biscuit"}
                                }
                            },
                            {"this", cvar::l{1, 2, "teeth", 4}}, // { 1,2,3,4 } },
                            {"feet", "mouth"}
                    };

                    ASSERT(anothercvar["things"]["hello"] == "goodbye");
                    ASSERT(anothercvar["this"][1] == 2);
                }
            },
            {
                "cvarblob: serialize/deserialize simple", []
                {
                    cvar inputcvar = 3.14;
                    HeapStack mem;
                    varBlob::serialize(mem, inputcvar);
                    auto serialData = mem.flatten();
                    cvar outputVar;
                    varBlob::deserialize(outputVar, serialData);
                    ASSERT(outputVar == 3.14);
                }
            },
            {
                "cvarblob: serialize/deserialize complex", []
                {
                    cvar inputcvar = {
                            {
                                "things", {
                                        {"hello"s, "goodbye"}, // mix some strings
                                    cvar::o{"tea", "biscuit"}
                                }
                            },
                            {"this", cvar::l{1, 2, "teeth", 4}}, // { 1,2,3,4 } },
                            {"feet", "mouth"},
                            {"stuff", cvar::s{"pig", "duck", 2}}
                    };

                    HeapStack mem;
                    cvar outputVar;

                    varBlob::serialize(mem, inputcvar);
                    const auto serialData = mem.flatten();
                    varBlob::deserialize(outputVar, serialData);
                    mem.releaseFlatPtr(serialData);
                    mem.reset();

                    ASSERT(outputVar["stuff"].contains("duck"));
                    ASSERT(outputVar["things"]["hello"] == "goodbye");
                    ASSERT(outputVar["this"][1] == 2);
                }
            },
            {
                "cvarblob: hash complex", []
                {
                    cvar inputcvar = {
                            {
                                "things", {
                                        {"hello"s, "goodbye"}, // mix some strings
                                    cvar::o{"tea", "biscuit"}
                                }
                            },
                            {"this", cvar::l{1, 2, "teeth", 4}}, // { 1,2,3,4 } },
                            {"feet", "mouth"},
                            {"stuff", cvar::s{"pig", "duck", 2}}
                    };

                    const auto hashBefore = varBlob::hash(inputcvar);
                    inputcvar["stuff"] += "added";

                    const auto hashAfter = varBlob::hash(inputcvar);

                    ASSERT(inputcvar["stuff"].contains("added"));
                    ASSERT(hashBefore != hashAfter);
                }
            },

    };
}
