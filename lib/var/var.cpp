#include "var.h"

size_t std::hash<cvar>::operator()(const cvar& v) const
{
	switch (v.typeOf())
	{
		case cvar::valueType::STR:
			return std::hash<std::string>{}(v.getString());
		case cvar::valueType::FLT:
		case cvar::valueType::DBL:
			return std::hash<double>{}(v.getDouble());
		default: 
			return v.getInt64();
	}
}

