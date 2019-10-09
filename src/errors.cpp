#include "errors.h"

openset::errors::Error::Error()
{}

openset::errors::Error::Error(
	const errorClass_e errorClass,
	const errorCode_e errorCode,
	const std::string errorDetail,
	const std::string errorAdditional) :
		eCode(errorCode),
		eClass(errorClass),
		message(string(openset::errors::errorStrings.at(errorCode))),
		classMessage(string(openset::errors::classStrings.at(errorClass))),
		detail(errorDetail),
		additional(errorAdditional)
{}

openset::errors::Error::~Error()
{}

void openset::errors::Error::set(
	const errorClass_e errorClass,
	const errorCode_e errorCode,
	const std::string errorDetail,
	const std::string errorAdditional)
{
	eCode = errorCode;
	eClass = errorClass;
	message = string(openset::errors::errorStrings.at(errorCode));
	classMessage = string(openset::errors::classStrings.at(errorClass));
	detail = errorDetail;
	additional = errorAdditional;
}

bool openset::errors::Error::inError() const
{
	return (eClass != errorClass_e::no_error);
}

std::string openset::errors::Error::getErrorJSON() const
{
	std::string json = "{\"error\":{";
	json += "\"class\":\"" + classMessage + "\",";
	json += "\"message\":\"" + message + "\"";
	if (detail.length())
		json += ",\"detail\":\"" + detail + "\"";
	if (additional.length())
		json += ",\"additional\":\"" + additional + "\"";
	return json + "}}";
}

