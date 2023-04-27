package com.bytesforce.pub

import org.apache.commons.lang3.StringUtils
import org.joda.time.DateMidnight
import org.joda.time.DateTime
import org.joda.time.Period
import org.joda.time.ReadableDateTime

import java.math.RoundingMode
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

final class MyDateUtils {
    private MyDateUtils() {}

    public static final String DEFAULT_DATE_FORMAT = "dd/MM/yyyy"
    public static final String DD_MM_YYYY_HH_MM_SS = "dd/MM/yyyy HH:mm:ss"
    public static final String MM_YYYY_FORMAT = "MM/yyyy"
    public static final String MMYYYY_FORMAT = "MMyyyy"
    public static final String ddMMMyyyy = "dd MMM yyyy"
    public static final String dd_MM_yyyy_HH_mm = "dd/MM/yyyy HH:mm"
    public static final String ddMMM = "dd MMM"
    public static final String yyyyMMdd = "yyyy-MM-dd"
    public static final String HHmmss = "HH:mm:ss"
    public static final String DEFAULT_DATETIME_WITH_TIMEZONE_FORMAT = "dd/MM/yyyy'T'HH:mm:ssXXX"
    public static final String ES_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
    public static final String MYSQL_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"
    public static final String YYYY_FORMAT = "yyyy"
    static Date MAX_DATE = toDate("9999-12-31", "yyyy-MM-dd")
    static Date MIN_DATE = toDate("1900-01-01", "yyyy-MM-dd")

    static String format(Date date, String format = DEFAULT_DATE_FORMAT) {
        if (date == null) {
            return null
        }
        date.format(format)
    }

    static final Integer ymdInt(def date) {
        if (date == null) {
            return null
        }
        if (!isDate(date)) {
            Panic.invalidParam("${date} is not a date value")
        }
        return format(toDate(date), "yyyyMMdd").toInteger()
    }

    static final Integer ymInt(def date) {
        if (date == null) {
            return null
        }
        if (!isDate(date)) {
            Panic.invalidParam("${date} is not a date value")
        }
        return format(toDate(date), "yyyyMM").toInteger()
    }

    static final Integer ymIntNow() {
        return format(now(), "yyyyMM").toInteger()
    }

    static final Long yyyyMMddHHmmssLong(def date) {
        if (date == null) {
            return null
        }
        if (!isDate(date)) {
            Panic.invalidParam("${date} is not a date value")
        }
        return format(toDate(date), "yyyyMMddHHmmss").toLong()
    }

    static final String yyyyMMddStr(def date) {
        if (date == null) {
            return null
        }
        if (!isDate(date)) {
            Panic.invalidParam("${date} is not a date value")
        }
        return format(toDate(date), "yyyyMMdd")
    }

    static final int getDaysOfYear(int year) {
        Calendar cal = Calendar.getInstance()
        cal.set(Calendar.YEAR, year)
        return cal.getActualMaximum(Calendar.DAY_OF_YEAR)
    }

    static final int getDaysOfMonth(Date date) {
        if (date == null) {
            return -1
        }
        return date.toCalendar().getActualMaximum(Calendar.DAY_OF_MONTH)
    }

    static Date getFirstDayOfMonth(Date date) {
        Calendar calendar = Calendar.getInstance()
        calendar.setTime(date)
        calendar.set(Calendar.DAY_OF_MONTH, calendar
                .getActualMinimum(Calendar.DAY_OF_MONTH))
        return calendar.getTime()
    }



    static Date getFirstDayOfCurrYear() {
        return jodaNow().withDayOfMonth(1).withMonthOfYear(1).toDate()
    }

    static Date getFirstDayOfCurrYear(Date date) {
        return new DateTime(date).withDayOfMonth(1).withMonthOfYear(1).toDate()
    }

    static Date getFirstDayOfLastYear() {
        return jodaNow().withDayOfYear(1).withMonthOfYear(1).minusYears(1).toDate()
    }

    static Date getFirstDayOfNextYear() {
        return jodaNow().withDayOfYear(1).withMonthOfYear(1).plusYears(1).toDate()
    }

    static Date getLastDayOfLastYear() {
        return toJodaDate(getFirstDayOfCurrYear()).minusDays(1).toDate()
    }

    static Date getLastDayOfCurrYear() {
        return jodaNow().withMonthOfYear(12).withDayOfMonth(31).toDate()
    }

    static Date getLastDayOfCurrYear(Date date) {
        return new DateTime(date).withMonthOfYear(12).withDayOfMonth(31).toDate()
    }

    static Date getLastDayOfNextYear() {
        return jodaNow().withMonthOfYear(12).withDayOfMonth(31).plusYears(1).toDate()
    }

    static Date getFirstDayOfCurrMonth() {
        return jodaNow().withDayOfMonth(1).toDate()
    }

    static Date getFirstDayOfNextMonth() {
        return jodaNow().withDayOfMonth(1).plusMonths(1).toDate()
    }

    static Date getLastDayOfMonth(Date date) {
        Calendar calendar = Calendar.getInstance()
        calendar.setTime(date)
        calendar.set(Calendar.DAY_OF_MONTH, calendar
                .getActualMaximum(Calendar.DAY_OF_MONTH))
        return calendar.getTime()
    }

    static Date getLastDayOfCurrMonth() {
        return getLastDayOfMonth(now())
    }

    static Date getLastDayOfLastMonth() {
        return getLastDayOfMonth(jodaNow().withDayOfMonth(1)
                .minusMonths(1).toDate())
    }

    static Date getFirstDayOfLastMonth() {
        return getFirstDayOfMonth(jodaNow().withDayOfMonth(1)
                .minusMonths(1).toDate())
    }

    static Date nvl(Date date, Date defaultValue = now()) {
        return date == null ? defaultValue : date
    }

    static String nullSafeFormat(Date date, String dateFormat = DEFAULT_DATE_FORMAT) {
        if (date == null) {
            return null
        }
        date.format(dateFormat)
    }

    static String ddMMMyyyyStr(Date d) {
        nullSafeFormat(d, ddMMMyyyy)
    }

    static String ddMMMStr(Date d) {
        nullSafeFormat(d, ddMMM)
    }

    /**
     * to detect whether the value is a date or not, if value is null, then return false
     * @param value
     * @param dateFormat default format please refer to Const.DEFAULT_DATE_FORMAT
     * @return
     */
    static boolean nullSafeIsDate(Object value, String dateFormat = DEFAULT_DATE_FORMAT) {
        if (value == null) {
            return false
        }
        return isDate(value, dateFormat)
    }

    /**
     * to detect whether the value is a date or not
     * @param value cannot be null
     * @param dateFormat default format please refer to Const.DEFAULT_DATE_FORMAT
     * @return
     */
    static boolean isDate(Object value, String dateFormat = DEFAULT_DATE_FORMAT) {
        assert value != null
        if (value instanceof Date) {
            return true
        }
        if (value instanceof ReadableDateTime) {
            return true
        }
        if (StringUtils.isBlank(value.toString())) {
            return false
        }
        def str = value.toString().trim()
        def sdf = new SimpleDateFormat(dateFormat)
        try {
            sdf.setLenient(false)
            sdf.parse(str)
            return true
        } catch (ParseException e) {
            return false
        }
    }

    static boolean isDateTime(Object value, String dateFormat = DEFAULT_DATETIME_WITH_TIMEZONE_FORMAT) {
        isDate(value, dateFormat)
    }

    /**
     * convert object value to Date value
     *
     * @param value the value to be converted to date
     * @param dateFormat default to "dd/MM/yyyy"
     * @return
     */
    static Date toDate(Object value, String dateFormat = DEFAULT_DATE_FORMAT) {
        if (value == null) {
            return null
        }
        if (value instanceof Date) {
            return value
        }
        if (value instanceof ReadableDateTime) {
            return value.toDateTime().toDate()
        }
        if (value.toString().trim() == "") {
            return null
        }
        def str = value.toString().trim()
        def sdf = new SimpleDateFormat(dateFormat)
        try {
            return sdf.parse(str)
        } catch (ParseException e) {
            return null
        }
    }

    static DateTime toJodaDate(Object value, String dateFormat = DEFAULT_DATE_FORMAT) {
        Date date = toDate(value, dateFormat)
        new DateTime(date)
    }

    /**
     * convert object value to Date value
     *
     * @param value the value to be converted to date
     * @param dateFormat default to DEFAULT_DATETIME_WITH_TIMEZONE_FORMAT
     * @return
     */
    static Date toDateTime(Object value, String dateFormat = DEFAULT_DATETIME_WITH_TIMEZONE_FORMAT) {
        toDate(value, dateFormat)
    }

    static Date truncateTime(Object date) {
        if (date == null) {
            return null
        }
        return new DateTime(toDate(date)).withTimeAtStartOfDay().toDate()
    }

    static Date add1Day(Object date) {
        if (date == null) {
            return null
        }
        return new DateTime(toDate(date)).withTimeAtStartOfDay().plusDays(1).toDate()
    }

    static Date addMonths(Object date, int addedMonths) {
        if (date == null) {
            return null
        }
        return new DateTime(toDate(date)).withTimeAtStartOfDay().plusMonths(addedMonths).toDate()
    }

    static Date add1Month(Object date) {
        return addMonths(date, 1)
    }

    /**
     * get list of YYYYMM int format months between the 2 dates
     * @param from
     * @param to
     * @return
     */
    static List<Integer> getYmMonthsBetween(Date from, Date to) {
        Date fr = getFirstDayOfMonth(from)
        Integer ymFr = ymInt(fr)
        Integer ymTo = ymInt(to)
        if (ymFr > ymTo) {
            return []
        } else if (ymFr == ymTo) {
            return [ymTo]
        } else {
            List<Integer> list = []
            int i = 0
            while (true) {
                int m = ymInt(addMonths(fr, i))
                if (m > ymTo) {
                    break
                }
                list.add(m)
                i++
            }
            return list
        }
    }

    static Date addYears(Object date, int years) {
        if (date == null) {
            return null
        }
        return new DateTime(toDate(date)).withTimeAtStartOfDay().plusYears(years).toDate()
    }

    static Date addDays(Object date, int days) {
        if (date == null) {
            return null
        }
        return new DateTime(toDate(date)).withTimeAtStartOfDay().plusDays(days).toDate()
    }

    static Date addMonth(Object date, int months) {
        if (date == null) {
            return null
        }
        return new DateTime(toDate(date)).withTimeAtStartOfDay().plusMonths(months).toDate()
    }

    /**
     * check whether the date is between min and max, the range cannot be null
     *
     * @param date
     * @param minDate
     * @param maxDate
     * @return
     */
    static boolean betweenIgnoreTime(Object date, Object minDate, Object maxDate) {
        Date d = truncateTime(toDate(date))
        Date min = truncateTime(toDate(minDate))
        Date max = truncateTime(toDate(maxDate))
        if (d == null) {
            return false
        }
        if (min == null) {
            assert min != null
        }
        if (max == null) {
            assert max != null
        }
        return d.time >= min.time && d.time <= max.time
    }

    /**
     * null safe method to check whether the date is between min and max
     *
     * @param date
     * @param minDate if null, default to new Date(0)
     * @param maxDate if null, default to new Date(Integer.MAX_VALUE)
     * @return
     */
    static boolean nullSafeBetweenIgnoreTime(Object date, Object minDate, Object maxDate) {
        Date d = toDate(date)
        Date min = toDate(minDate)
        Date max = toDate(maxDate)
        if (d == null) {
            return false
        }
        if (min == null) {
            min = new Date(0)
        }
        if (max == null) {
            max = new Date(Integer.MAX_VALUE)
        }
        return betweenIgnoreTime(d, min, max)
    }

    /**
     * compare 2 date variable ignore time
     *
     * @param date1
     * @param date2
     * @return 1: date1 > date2, 0: date1 == date2, -1: date1 < date2
     */
    static int compareIgnoreTime(Object date1, Object date2) {
        assert date1 != null
        assert date2 != null
        if (!isDate(date1)) {
            Panic.invalidParam("compareIgnoreTime: date1 [${date1}] is not a date value")
        }
        if (!isDate(date2)) {
            Panic.invalidParam("compareIgnoreTime: date2 [${date2}] is not a date value")
        }
        long t1 = truncateTime(date1).time
        long t2 = truncateTime(date2).time
        if (t1 < t2) {
            return -1
        } else if (t1 == t2) {
            return 0
        } else {
            return 1
        }
    }

    static boolean isTodayOrLater(Date date) {
        compareIgnoreTime(date, now()) >= 0
    }

    static BigDecimal actualYearsBetweenIgnoreTime(Object from, Object to = now()) {
        BigDecimal months = monthsBetweenIgnoreTime(toDate(from), toDate(to))
        return MyNumberUtils.round2(months / 12)
    }

    static BigDecimal monthsBetweenIgnoreTime(Date from, Date to) {
        DateTime startDate = new DateTime(from).toDateMidnight().toDateTime()
        DateTime endDate = new DateTime(to).toDateMidnight().toDateTime()
        if (startDate.plusYears(1).minusDays(1).millis == endDate.millis) {
            return 12
        }
        def years = endDate.getYear() - startDate.getYear()
        def months = endDate.getMonthOfYear() - startDate.getMonthOfYear()
        def days
        if (endDate.getDayOfMonth() == startDate.getDayOfMonth()
                || ((startDate.toDate().toCalendar().getActualMaximum(Calendar.DAY_OF_MONTH) == startDate.getDayOfMonth()
                && endDate.toDate().toCalendar().getActualMaximum(Calendar.DAY_OF_MONTH) == endDate.getDayOfMonth()))) {
            days = 0.0
        } else {
            days = endDate.getDayOfMonth() - startDate.getDayOfMonth()
        }
        BigDecimal monthAmount = years * 12 + months + days / 30
        return monthAmount
    }

    static int daysBetweenIgnoreTime(Date from, Date to) {
        if (!from || !to) {
            throw new IllegalArgumentException("daysBetweenIgnoreTime: both [from] or [to] are not allow to be null")
        }
        long timeDiff = new DateMidnight(to).toDate().time - new DateMidnight(from).toDate().time
        (timeDiff / 86400000).setScale(0, RoundingMode.HALF_UP)
    }

    static BigDecimal actualDaysBetween(Date from, Date to, int decimal = 2) {
        if (!from || !to) {
            return null
        }
        decimal = decimal >= 0 && decimal <= 4 ? decimal : 2
        long timeDiff = new DateMidnight(to).toDate().time - new DateMidnight(from).toDate().time
        (Math.abs(timeDiff) / 86400000).setScale(decimal, RoundingMode.HALF_UP)
    }

    static int hoursBetween(Date from, Date to) {
        if (!from || !to) {
            throw new IllegalArgumentException("hoursBetweenIgnoreTime: both [from] or [to] are not allow to be null")
        }
        long timeDiff = to.time - from.time
        (timeDiff / 1000 / 60 / 60).setScale(0, RoundingMode.FLOOR)
    }

    static BigDecimal actualMinutesBetween(Date from, Date to, int decimal = 2) {
        if (!from || !to) {
            return null
        }
        long timeDiff = Math.abs(to.time - from.time)
        decimal = decimal >= 0 && decimal <= 4 ? decimal : 2
        (timeDiff / 1000 / 60).setScale(decimal, RoundingMode.HALF_UP)
    }

    private static volatile int DAYS_OFFSET = 0

    /**
     * travel system time to X days ago or later, only applicable to unit test
     * @param days
     *  negative days: travel to abs(X) days ago<br>
     *      positive days: travel to X days later<br>
     *          0: no time travel
     */
    static final void timeTravel(int days) {
        if (EnvUtils.prod()) {
            Panic.notSupported("Time Traveling feature is designed for test purpose, cannot be executed on PROD environment")
        }
        DAYS_OFFSET = days
    }

    static final void timeTravel(Date travelTo) {
        if (travelTo == null) {
            return
        }
        resetTimeTravel()
        int days = daysBetweenIgnoreTime(today(), travelTo)
        timeTravel(days)
    }

    static final void resetTimeTravel() {
        timeTravel(0)
    }

    static final Integer ymdIntNow() {
        ymdInt(today())
    }

    static final Integer ymdIntTomorrow() {
        ymdInt(tomorrow())
    }

    static Date now() {
        if (DAYS_OFFSET == 0) {
            new Date()
        } else {
            DateTime.now().plusDays(DAYS_OFFSET).toDate()
        }
    }

    static DateTime jodaNow() {
        new DateTime(now())
    }

    static DateTime jodaTomorrow() {
        new DateTime(tomorrow())
    }

    static Date today() {
        jodaNow().withTimeAtStartOfDay().toDate()
    }

    static Date yesterday() {
        jodaNow().minusDays(1).withTimeAtStartOfDay().toDate()
    }

    static Date tomorrow() {
        jodaNow().plusDays(1).withTimeAtStartOfDay().toDate()
    }

    static int years(Object from, Object to = now()) {
        if (from == null) {
            throw new IllegalArgumentException("Date value 'from' is null, couldn't continue to calculate years between")
        }
        if (to == null) {
            throw new IllegalArgumentException("Date value 'to' is null, couldn't continue to calculate years between")
        }
        Date birthday
        if (from instanceof Date) {
            birthday = from
        } else if (isDate(from)) {
            birthday = toDate(from)
        } else {
            throw new IllegalArgumentException("Invalid format of Date of birth, couldn't continue to calculate age")
        }

        Date today
        if (to instanceof Date) {
            today = to
        } else if (isDate(from)) {
            today = toDate(to)
        } else {
            throw new IllegalArgumentException("Invalid format of Date value 'to', couldn't continue to calculate years between")
        }

        if (birthday.after(today)) {
            throw new IllegalArgumentException("the date value 'from' can't be after 'to'")
        }

        def period = new Period(new DateTime(birthday), new DateTime(today))
        return period.getYears()
    }

    static Date toThaiDate(Date date = null) {
        int plusYears = 543
        if (!date) {
            return jodaNow().plusYears(plusYears).toDate()
        }
        return new DateTime(date.getTime()).plusYears(plusYears).toDate()
    }

    static Date getLastSaturday(Date date = now()) {
        if (date == null) {
            return null
        }
        DateTime d = new DateTime(date)

        if (d.dayOfWeek == 7) {
            return d.minusDays(1).toDate()
        } else if (d.dayOfWeek == 6) {
            return d.minusWeeks(1).toDate()
        } else {
            d = d.minusWeeks(1)
            int offset = 6 - d.dayOfWeek
            return d.plusDays(offset).toDate()
        }
    }

    static Date getLastSunday(Date date = now()) {
        if (date == null) {
            return null
        }
        DateTime d = new DateTime(date)

        if (d.dayOfWeek == 1) {
            return d.minusDays(1).toDate()
        } else if (d.dayOfWeek == 7) {
            return d.minusWeeks(1).toDate()
        } else {
            d = d.minusWeeks(1)
            int offset = 7 - d.dayOfWeek
            return d.plusDays(offset).toDate()
        }
    }

    static boolean isOverlapped(DateRange range1, DateRange range2) {
        assert range1 != null
        assert range1.from != null
        assert range1.to != null
        assert range2 != null
        assert range2.from != null
        assert range2.to != null

        range1.from = truncateTime(range1.from)
        range1.to = truncateTime(range1.to)
        range2.from = truncateTime(range2.from)
        range2.to = truncateTime(range2.to)

        if (betweenIgnoreTime(range1.from, range2.from, range2.to)) {
            return true
        }
        if (betweenIgnoreTime(range1.to, range2.from, range2.to)) {
            return true
        }
        return false
    }

    static boolean isTheSameRange(DateRange range1, DateRange range2) {
        assert range1 != null
        assert range1.from != null
        assert range1.to != null
        assert range2 != null
        assert range2.from != null
        assert range2.to != null

        range1.from = truncateTime(range1.from)
        range1.to = truncateTime(range1.to)
        range2.from = truncateTime(range2.from)
        range2.to = truncateTime(range2.to)

        return format(range1.from) == format(range2.from) &&
                format(range1.to) == format(range2.to)
    }

    static boolean hasAGap(DateRange range1, DateRange range2) {
        assert range1 != null
        assert range1.from != null
        assert range1.to != null
        assert range2 != null
        assert range2.from != null
        assert range2.to != null

        range1.from = truncateTime(range1.from)
        range1.to = truncateTime(range1.to)
        range2.from = truncateTime(range2.from)
        range2.to = truncateTime(range2.to)

        return new DateTime(range1.to).plusDays(1).toDate().format("yyyyMMdd") !=
                range2.from.format("yyyyMMdd")
    }

    static boolean isSubset(DateRange range1, DateRange range2) {
        assert range1 != null
        assert range1.from != null
        assert range1.to != null
        assert range2 != null
        assert range2.from != null
        assert range2.to != null

        range1.from = truncateTime(range1.from)
        range1.to = truncateTime(range1.to)
        range2.from = truncateTime(range2.from)
        range2.to = truncateTime(range2.to)

        return betweenIgnoreTime(range1.from, range2.from, range2.to) &&
                betweenIgnoreTime(range1.to, range2.from, range2.to)
    }

    public static final List<Long> times = Arrays.asList(
            TimeUnit.DAYS.toMillis(365),
            TimeUnit.DAYS.toMillis(30),
            TimeUnit.DAYS.toMillis(1),
            TimeUnit.HOURS.toMillis(1),
            TimeUnit.MINUTES.toMillis(1),
            TimeUnit.SECONDS.toMillis(1));
    public static final List<String> timesString = Arrays.asList("year", "month", "day", "hour", "minute", "second");

    static final String toDuration(long duration) {
        StringBuilder res = new StringBuilder()
        for (int i = 0; i < times.size(); i++) {
            Long current = times.get(i)
            long temp = (duration / current)?.longValue()
            if (temp > 0) {
                res.append(temp).append(" ").append(timesString.get(i)).append(temp == 1 ? "" : "s")
                break
            }
        }
        if (res.length() == 0) {
            return "0 second"
        } else {
            return res.toString()
        }
    }

    static final String getReadableQuarter(Date date) {
        if (date == null) {
            return null
        }
        def dt = new DateTime(date)
        int year = dt.getYear()
        int month = dt.getMonthOfYear()
        if (month <= 3 && month >= 1) {
            return "${year}Q1".toString()
        } else if (month <= 6 && month >= 4) {
            return "${year}Q2".toString()
        } else if (month <= 9 && month >= 7) {
            return "${year}Q3".toString()
        } else {
            return "${year}Q4".toString()
        }
    }

    static int getQuarter(Date date) {
        int quarter = 0
        if (date == null) {
            return quarter
        }
        def dt = new DateTime(date)
        int year = dt.getYear()
        int month = dt.getMonthOfYear()
        if (month <= 3 && month >= 1) {
            quarter = 1
            return quarter
        } else if (month <= 6 && month >= 4) {
            quarter = 2
            return quarter
        } else if (month <= 9 && month >= 7) {
            quarter = 3
            return quarter
        } else {
            quarter = 4
            return quarter
        }
    }

    //当前季度的第一刻
    static Date getFirstDayOfCurrQuarter() {
        //todo: use getFirstDayOfQuarterOf
        int month = jodaNow().getMonthOfYear()
        int quarter = (int) ((month + 2) / 3)
        int firstMonthCurrentQuarter = (quarter - 1) * 3 + 1
        return new DateMidnight(DateTime.now().withMonthOfYear(firstMonthCurrentQuarter).withDayOfMonth(1)).toDate()
    }

    static Date getFirstDayFirstSecondOfCurrQuarter(Date date) {
        //todo: use getFirstDayOfQuarterOf
        DateTime dateTime = new DateTime(date)
        int month = dateTime.getMonthOfYear()
        int quarter = (int) ((month + 2) / 3)
        int firstMonthCurrentQuarter = (quarter - 1) * 3 + 1
        return new DateMidnight(dateTime.withMonthOfYear(firstMonthCurrentQuarter).withDayOfMonth(1)).toDate()
    }

    static Date getLastDayLastSecondOfCurrQuarter(Date date) {
        return new DateTime(getFirstDayFirstSecondOfCurrQuarter(date)).plusMonths(3).minusSeconds(1).toDate()
    }
}
