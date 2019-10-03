#pragma once

#include "include/libcommon.h"
#include <ctime>

class Epoch
{
public:

    // ensure a stamp is milliseconds since epoch
    static constexpr int64_t fixMilli(int64_t stamp)
    {
        // if less than the year 2100 add milliseconds
        return stamp < 4102444800 ? stamp * 1000LL : stamp;
    }

    // ensure the stamp is just seconds since epoch
    static constexpr int64_t fixUnix(int64_t stamp)
    {
        // if less than the year 2100 add milliseconds
        return stamp < 4102444800 ? stamp : stamp / 1000LL;
    }

    static constexpr int64_t getMilli(int64_t stamp)
    {
        return fixMilli(stamp) % 1000;
    }

    // standard time functions use a global structure and are not thread safe
    static inline void stampToTimeStruct(struct tm* timeStruct, int64_t stamp)
    {
#ifdef _MSC_VER
        _gmtime64_s(timeStruct, &stamp);
#else
        gmtime_r(&stamp, timeStruct);
#endif
    }

    static int64_t epochSecondNumber(int64_t stamp)
    {
        stamp = fixUnix(stamp);
        struct tm time;
        stampToTimeStruct(&time, stamp);
        return time.tm_sec;
    }

    static int64_t epochSecondDate(int64_t stamp)
    {
        stamp = fixUnix(stamp);
        return stamp;
    }

    static int64_t epochMinuteDate(int64_t stamp)
    {
        stamp = fixUnix(stamp);
        return static_cast<int64_t>(stamp / 60) * 60;
    }

    static int64_t epochMinuteNumber(int64_t stamp)
    {
        stamp = fixUnix(stamp);
        struct tm time;
        stampToTimeStruct(&time, stamp);
        return time.tm_min;
    }

    static int64_t epochHourNumber(int64_t stamp)
    {
        stamp = fixUnix(stamp);
        struct tm time;
        stampToTimeStruct(&time, stamp);
        return time.tm_hour;
    }

    static int64_t epochHourDate(int64_t stamp)
    {
        stamp = fixUnix(stamp);
        return static_cast<int64_t>(stamp / 3600) * 3600;
    }


    static int64_t epochDayDate(int64_t stamp)
    {
        stamp = fixUnix(stamp);
        return static_cast<int64_t>(stamp / 86400) * 86400;
    }

    static int64_t epochWeekDate(int64_t stamp)
    {
        stamp = fixUnix(stamp);
        struct tm time;
        stampToTimeStruct(&time, stamp);
        stamp = static_cast<int64_t>(stamp / 86400) * 86400;
        stamp -= time.tm_wday * 86400;
        return stamp;
    }

    static inline int64_t epochMonthNumber(int64_t stamp)
    {
        stamp = fixUnix(stamp);
        struct tm time;
        stampToTimeStruct(&time, stamp);
        return time.tm_mon + 1;
    }

    static int64_t epochMonthDate(int64_t stamp)
    {
        stamp = fixUnix(stamp);
        struct tm time;
        stampToTimeStruct(&time, stamp);
        stamp = static_cast<int64_t>(stamp / 86400) * 86400;
        stamp -= (time.tm_mday - 1) * 86400;
        return stamp;
    }

    static int64_t epochQuarterNumber(int64_t stamp)
    {
        stamp = fixUnix(stamp);
        struct tm time;
        stampToTimeStruct(&time, stamp);
        auto q = static_cast<int64_t>(time.tm_mon / 3) * 3;
        return q + 1;
    }

    static int64_t epochQuarterDate(int64_t stamp)
    {
        stamp = fixUnix(stamp);
        struct tm time;
        stampToTimeStruct(&time, stamp);
        auto SOY = static_cast<int64_t>(stamp / 86400) * 86400;
        SOY -= time.tm_yday * 86400;

        stampToTimeStruct(&time, stamp);
        auto q = static_cast<int64_t>(time.tm_mon / 3) * 3;
        q *= (86400 * 31);

        stamp = SOY + q;
        stampToTimeStruct(&time, stamp + q);

        stamp -= (time.tm_mday - 1) * 86400;
        return stamp;
    }

    static int64_t epochDayOfWeek(int64_t stamp)
    {
        stamp = fixUnix(stamp);
        struct tm time;
        stampToTimeStruct(&time, stamp);
        return time.tm_wday + 1;
    }

    static int64_t epochDayOfMonth(int64_t stamp)
    {
        stamp = fixUnix(stamp);
        struct tm time;
        stampToTimeStruct(&time, stamp);
        return time.tm_mday;
    }

    static int64_t epochDayOfYear(int64_t stamp)
    {
        stamp = fixUnix(stamp);
        struct tm time;
        stampToTimeStruct(&time, stamp);
        return time.tm_yday + 1;
    }

    static int64_t epochYearNumber(int64_t stamp)
    {
        struct tm time;
        stampToTimeStruct(&time, stamp);
        return time.tm_year + 1900;
    }

    static int64_t epochYearDate(int64_t stamp)
    {
        stamp = fixUnix(stamp);
        struct tm time;
        stampToTimeStruct(&time, stamp);
        stamp = static_cast<int64_t>(stamp / 86400) * 86400;
        stamp -= time.tm_yday * 86400;
        return stamp;
    }



    /*  ISO8601 date detect/parser
     * 
     *  Supported formats:
     *		 
     *		yyyy-mm-ddThh:mm:ssZ          - GMT/UTC/Zulu time 
     *      yyyy-mm-ddThh:mm:ss+00:00     - Zone offset
     *      yyyy-mm-ddThh:mm:ss.mmm+00:00 - Zone offset and decimial milliseconds
     *
     *  specifying future (in milliseconds) cause ISO8601ToEpoch to fail if
     *  a date is the specified milliseconds into the future.
     *	
     *	returns -1 on error
     */

    static bool isISO8601(const std::string& time)
    {
        return (
            time.length() >= 19 &&
            time[4] == '-' && 
            time[7] == '-' && 
            time[13] == ':' &&
            time[16] == ':');
    }

    static int64_t ISO8601ToEpoch(const std::string& time)
    {
        // parse the date...
        std::string temp;

        if (time.length() < 19)
            return -1;
        
        // look for punctuation
        if (time[4] != '-' || 
            time[7] != '-' || 
            time[13] != ':' ||
            time[16] != ':')
            return -1;

        auto milliseconds = 0, zone_hours = 0, zone_minutes = 0;

        const auto year = stoi(time.substr(0, 4));
        const auto month = stoi(time.substr(5, 2));
        const auto day = stoi(time.substr(8, 2));
        const auto hour = stoi(time.substr(11, 2));
        const auto minute = stoi(time.substr(14, 2));
        const auto second = stoi(time.substr(17, 2));

        // we have millis
        if (time[19] == '.')
            milliseconds = stoi(time.substr(20, time.npos - 20));

        auto negative = false;

        // if there is no Z or null terminate
        auto zonePos = time.find('+',19);
        if (zonePos == time.npos)
            zonePos = time.find('-', 19);

        if (zonePos != time.npos) // we have zone
        {
            negative = (time[zonePos] == '-');
            zone_hours = stoi(time.substr(zonePos+1, 2));
            zone_minutes = stoi(time.substr(zonePos +4, 2));
        }

        struct tm t = { 0 };
        t.tm_sec = second;
        t.tm_min = minute;
        t.tm_hour = hour;
        t.tm_mday = day;
        t.tm_mon = month - 1;
        t.tm_year = year - 1900;

#ifdef _MSC_VER
        auto stamp = static_cast<int64_t>(_mkgmtime64(&t));
#else
        auto stamp = static_cast<int64_t>(timegm(&t));
#endif
    
        // add zone
        if (zone_hours || zone_minutes)
            stamp -= (negative ? -1 : 1) * ((zone_hours * 3600) + (zone_minutes * 60));

        return (stamp * 1000) + milliseconds;
    }

    static std::string EpochToISO8601(int64_t epoch)
    {

        // makes this: yyyy-mm-ddThh:mm:ss.mmmZ

        const auto pad2 = [](int number)
        {
            auto str = std::to_string(number);
            if (str.length() < 2)
                str = "0" + str;
            return str;
        };

        const auto padMilli = [](int64_t number)
        {
            auto str = std::to_string(number);
            while (str.length() < 3)
                str = "0" + str;
            return str;
        };

        epoch = fixMilli(epoch);

        auto milliseconds = epoch % 1000;

        epoch = fixUnix(epoch);
        struct tm time;
        stampToTimeStruct(&time, epoch);
        
        auto iso =
            std::to_string(time.tm_year + 1900) + '-' +
            pad2(time.tm_mon + 1) + '-' +
            pad2(time.tm_mday) + 'T' +
            pad2(time.tm_hour) + ':' +
            pad2(time.tm_min) + ':' +
            pad2(time.tm_sec);

        if (milliseconds)
            iso += "." + padMilli(milliseconds);

        iso += "Z";

        return iso;		
    }


};
