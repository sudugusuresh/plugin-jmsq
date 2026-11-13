/*
 * Copyright 2026 SKACT Tech
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.plugin.jmsq.util;

import java.io.FileInputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Static helpers shared by the queue connectors: number parsing with defaults,
 * property-file loading, date formatting, connector-string parsing, string
 * escaping and log-line building.
 * <p>
 * The {@code addToBufferLog} methods separate fields with {@link #delimiter}.
 * It is chosen once, when the class loads:
 * <ul>
 * <li>If the system property {@code qmanager.logger.enable} is {@code "1"} (the
 * default), the delimiter is the value of the system property
 * {@code java.logger.delimiter}, or the control character {@code 0x16} if that
 * property is not set.</li>
 * <li>Otherwise the delimiter is a comma.</li>
 * </ul>
 * <p>
 * The shared {@link SimpleDateFormat} instances are not thread-safe, so the
 * date methods should not be called concurrently.
 */
public class QUtil {


    /** Event ID raised when a queue is created. */
    public final static int QCREATE_EVENTID = 1000;
    /** Event ID raised when a queue is deleted. */
    public final static int QDELETE_EVENTID = 1001;
    /** Event ID raised when a queue becomes inactive. */
    public final static int QINACTIVE_EVENTID = 1002;
    /** Event name for queue creation. */
    public final static String QCREATE_EVENT = "QCREATE";
    /** Event name for queue deletion. */
    public final static String QDELETE_EVENT = "QDELETE";
    /** Event name for an inactive queue. */
    public final static String QINACTIVE_EVENT = "QINACTIVE";
    /** Event severity: critical. Severities are bit flags. */
    public final static int SEVERITY_CRITICAL = 1;
    /** Event severity: debug. */
    public final static int SEVERITY_DEBUG     = 2;
    /** Event severity: warning. */
    public final static int SEVERITY_WARNING  = 4;
    /** Event severity: informational. */
    public final static int SEVERITY_INFO   = 8;
    static Logger LOGGER = LogManager.getLogger(QUtil.class);

    /** Field separator used by the {@code addToBufferLog} methods; see the class description. */
    static String delimiter = ",";

    static {
        boolean m_bJavaLoggerEnabled = System.getProperty("qmanager.logger.enable", "1").equalsIgnoreCase("1");
        String tmpdelimiter = System.getProperty("java.logger.delimiter");
        if (m_bJavaLoggerEnabled) {
            byte b = 0X16;
            char c = (char) b;
            if (tmpdelimiter == null) {
                delimiter = "" + c;
            } else {
                delimiter = tmpdelimiter;
            }
        }
    }

    /**
     * Parses an {@code int}, returning a default if parsing fails.
     *
     * @param strVal   text to parse; may be {@code null}
     * @param nDefault value returned if {@code strVal} is not a valid integer
     * @return the parsed value, or {@code nDefault}
     */
    public static int intVal(String strVal, int nDefault) {
        try {
            return Integer.parseInt(strVal);
        } catch (Exception ex) {
            return nDefault;
        }
    }

    /**
     * Parses a {@code long}, returning a default if parsing fails.
     *
     * @param strVal   text to parse; may be {@code null}
     * @param nDefault value returned if {@code strVal} is not a valid long
     * @return the parsed value, or {@code nDefault}
     */
    public static long longVal(String strVal, long nDefault) {
        try {
            return Long.parseLong(strVal);
        } catch (Exception ex) {
            return nDefault;
        }
    }

    /**
     * Loads a properties file into an existing {@link Properties} object.
     *
     * @param strPropertyFile path of the file to load
     * @param props           object to load into; existing keys are overwritten
     * @return {@code props} on success, or {@code null} if the file could not
     *         be read (the error is printed to standard output)
     */
    public static Properties loadProperties(String strPropertyFile, Properties props) {
        FileInputStream fin = null;
        try {
            fin = new FileInputStream(strPropertyFile);
            props.load(fin);
            return props;
        } catch (Exception ex) {
            System.out.println("loadProperties() Failed. Ex : " + ex.getMessage());
        } finally {
            try {
                fin.close();
            } catch (Exception e) {
            }
        }
        return null;
    }
    static SimpleDateFormat sdfWithMilli = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * Formats the current time.
     *
     * @param bIncludeMilliSeconds {@code true} for {@code yyyy-MM-dd HH:mm:ss.SSS},
     *                             {@code false} for {@code yyyy-MM-dd HH:mm:ss}
     * @return the formatted current time
     */
    public static String now(boolean bIncludeMilliSeconds) {
        if (bIncludeMilliSeconds) {
            return sdfWithMilli.format(new java.util.Date());
        } else {
            return sdf.format(new java.util.Date());
        }
    }

    /**
     * Formats a date.
     *
     * @param bIncludeMilliSeconds {@code true} for {@code yyyy-MM-dd HH:mm:ss.SSS},
     *                             {@code false} for {@code yyyy-MM-dd HH:mm:ss}
     * @param d                    date to format
     * @return the formatted date
     */
    public static String now(boolean bIncludeMilliSeconds, Date d) {
        if (bIncludeMilliSeconds) {
            return sdfWithMilli.format(d);
        } else {
            return sdf.format(d);
        }
    }

    /**
     * Parses a date string.
     *
     * @param bIncludeMilliSeconds {@code true} if {@code d} is in
     *                             {@code yyyy-MM-dd HH:mm:ss.SSS} format,
     *                             {@code false} for {@code yyyy-MM-dd HH:mm:ss}
     * @param d                    text to parse
     * @return the parsed date, or {@code null} if parsing fails (the error is logged)
     */
    public static Date getDate(boolean bIncludeMilliSeconds, String d) {
        try {
            if (bIncludeMilliSeconds) {
                return sdfWithMilli.parse(d);
            } else {
                return sdf.parse(d);
            }
        } catch (ParseException ex) {
        	LOGGER.error("getDate parse is failed for datestring "+d, ex);
        }
        return null;
    }
    /**
     * Formats an epoch timestamp as {@code yyyy-MM-dd HH:mm:ss}.
     *
     * @param lMilliSeconds milliseconds since the epoch
     * @return the formatted date, or an empty string on error
     */
    public static String getDateString(long lMilliSeconds) {
        try {
            Date d = new Date(lMilliSeconds);
            return sdf.format(d);
            
        } catch (Exception ex) {
        	LOGGER.error("getDateString is failed for millis "+lMilliSeconds, ex);
        }
        return "";
    }

    /**
     * Returns whichever of two date strings is later.
     *
     * @param strDate1             first date
     * @param strDate2             second date
     * @param bIncludeMilliSeconds {@code true} if both dates are in
     *                             {@code yyyy-MM-dd HH:mm:ss.SSS} format,
     *                             {@code false} for {@code yyyy-MM-dd HH:mm:ss}
     * @return {@code strDate2} if it is later than {@code strDate1}; otherwise
     *         {@code strDate1}, which is also returned if either date cannot be parsed
     */
    public static String compareAndGetMaxDate(String strDate1, String strDate2, boolean bIncludeMilliSeconds) {
        String maxdate = strDate1;
        try {
            Date date1 = bIncludeMilliSeconds ? sdfWithMilli.parse(strDate1) : sdf.parse(strDate1);
            Date date2 = bIncludeMilliSeconds ? sdfWithMilli.parse(strDate2) : sdf.parse(strDate2);
            if (date1.before(date2)) {
                maxdate = strDate2;
            }
        } catch (Exception e) {
        }
        return maxdate;
    }

    /**
     * Appends an {@code int}, optionally followed by a comma.
     *
     * @param strBuff      buffer to append to
     * @param nVal         value to append
     * @param bAppendComma {@code true} to append a comma after the value
     * @return {@code strBuff}, for chaining
     */
    public static StringBuffer addToBuffer(StringBuffer strBuff, int nVal, boolean bAppendComma) {
        strBuff.append(nVal);
        if (bAppendComma) {
            strBuff.append(",");
        }
        return strBuff;
    }

    /**
     * Appends a {@code long}, optionally followed by a comma.
     *
     * @param strBuff      buffer to append to
     * @param nVal         value to append
     * @param bAppendComma {@code true} to append a comma after the value
     * @return {@code strBuff}, for chaining
     */
    public static StringBuffer addToBuffer(StringBuffer strBuff, long nVal, boolean bAppendComma) {
        strBuff.append(nVal);
        if (bAppendComma) {
            strBuff.append(",");
        }
        return strBuff;
    }

    /**
     * Escapes a string MySQL-style so it can be embedded in a quoted SQL
     * literal or written on one log line.
     * <p>
     * Escapes NUL, newline, carriage return, backslash, double quote, single
     * quote and Ctrl-Z ({@code \032}) with a leading backslash. This is not a
     * substitute for prepared statements.
     *
     * @param s text to escape; may be {@code null}
     * @return the escaped text, or an empty string if {@code s} is {@code null} or empty
     */
    public static String strEscape(String s) {
        if (s == null || s.length() <= 0) {
            return "";
        }
        boolean bConvert = false;
        StringBuffer sb = new StringBuffer(s);
        for (int i = 0; i < sb.length(); i++) {
            char c = sb.charAt(i);
            switch (c) {
                case '\0':
                    sb.setCharAt(i++, '\\');
                    sb.insert(i, "0"); // Must be escaped for "mysql"
                    break;
                case '\n':
                    sb.setCharAt(i++, '\\');
                    sb.insert(i, "n");// Must be escaped for logs
                    break;
                case '\r':
                    sb.setCharAt(i++, '\\');
                    sb.insert(i, "r");
                    break;
                case '\\':
                    sb.setCharAt(i++, '\\');
                    sb.insert(i, "\\");
                    break;
                case '\"':
                    sb.setCharAt(i++, '\\');
                    sb.insert(i, "\"");
                    break;
                case '\'':
                    sb.setCharAt(i++, '\\');
                    sb.insert(i, "\'");// Better safe than sorry
                    break;
                case '\032':
                    sb.setCharAt(i++, '\\');
                    sb.insert(i, "Z"); // This gives problems on Win32
                    break;
                default:
                    break;
            }
        }
        return sb.toString();
    }

    /**
     * Parses several connector strings separated by {@code |}.
     * <p>
     * Example: {@code "host=a&port=1|host=b&port=2"} gives two maps.
     *
     * @param strExtraparams connector strings separated by {@code |}; must not be {@code null}
     * @return one map per connector string; see {@link #getExtraparamslist(String)}
     */
    public static ArrayList<Hashtable> getExtraparamslistMap(String strExtraparams) {
        String[] arr = strExtraparams.split("\\|");
        ArrayList<Hashtable> al = new ArrayList<Hashtable>();
        for (int i = 0; i < arr.length; i++) {
            String url = arr[i];
            Hashtable table = new Hashtable();
            StringTokenizer stzExtraparmas = new StringTokenizer(url, "&");
            while (stzExtraparmas.hasMoreTokens()) {
                StringTokenizer stzExtparam = new StringTokenizer(stzExtraparmas.nextToken(), "=");
                if (stzExtparam.countTokens() == 2) {
                    String key = stzExtparam.nextToken();
                    String value = stzExtparam.nextToken();
                    table.put(key, value);
                }
            }
            al.add(table);
        }
        return al;
    }

    /**
     * Parses a connector string of the form {@code key1=value1&key2=value2}
     * into a map.
     * <p>
     * Pairs that do not split into exactly one key and one value are skipped,
     * so values must not contain {@code =} or {@code &} and empty values are
     * dropped.
     *
     * @param strExtraparams connector string; must not be {@code null}
     * @return map of keys to values (both strings)
     */
    public static Hashtable getExtraparamslist(String strExtraparams) {
        Hashtable table = new Hashtable();
        StringTokenizer stzExtraparmas = new StringTokenizer(strExtraparams, "&");
        while (stzExtraparmas.hasMoreTokens()) {
            StringTokenizer stzExtparam = new StringTokenizer(stzExtraparmas.nextToken(), "=");
            if (stzExtparam.countTokens() == 2) {
                String key = stzExtparam.nextToken();
                String value = stzExtparam.nextToken();
                table.put(key, value);
            }
        }
        return table;
    }

    /**
     * Appends a string wrapped in single quotes, optionally followed by
     * {@link #delimiter}. Single quotes inside the value become double quotes.
     *
     * @param strBuff      buffer to append to
     * @param strVal       value to append; {@code null} is written as {@code ''}
     * @param bAppendComma {@code true} to append the delimiter after the value
     * @return {@code strBuff}, for chaining
     */
    public static StringBuffer addToBufferLog(StringBuffer strBuff, String strVal, boolean bAppendComma) {
        if (strVal != null) {
            strVal = strVal.replaceAll("\\'", "\"");
        } else {
            strVal = "";
        }
        strBuff.append("\'" + strVal + "\'");
        if (bAppendComma) {
            strBuff.append(delimiter);
        }
        return strBuff;
    }

    /**
     * Appends an {@code int}, optionally followed by {@link #delimiter}.
     *
     * @param strBuff      buffer to append to
     * @param nVal         value to append
     * @param bAppendComma {@code true} to append the delimiter after the value
     * @return {@code strBuff}, for chaining
     */
    public static StringBuffer addToBufferLog(StringBuffer strBuff, int nVal, boolean bAppendComma) {
        strBuff.append(nVal);
        if (bAppendComma) {
            strBuff.append(delimiter);
        }
        return strBuff;
    }

    /**
     * Appends a {@code long}, optionally followed by {@link #delimiter}.
     *
     * @param strBuff      buffer to append to
     * @param nVal         value to append
     * @param bAppendComma {@code true} to append the delimiter after the value
     * @return {@code strBuff}, for chaining
     */
    public static StringBuffer addToBufferLog(StringBuffer strBuff, long nVal, boolean bAppendComma) {
        strBuff.append(nVal);
        if (bAppendComma) {
            strBuff.append(delimiter);
        }
        return strBuff;
    }

    /**
     * Returns the whole minutes elapsed since a timestamp.
     *
     * @param datetime earlier time, in milliseconds since the epoch
     * @return minutes between {@code datetime} and now, rounded down
     */
    public static long getDiffMinutes(long datetime) {
        long current=System.currentTimeMillis();
        return (((current-datetime)/1000)/60);
    }
    /**
     * Formats an epoch timestamp as {@code yyyy-MM-dd HH:mm:ss}. Thread-safe,
     * because it creates its own formatter on each call.
     *
     * @param currentDateTime milliseconds since the epoch
     * @return the formatted date
     */
    public static String millisToDate(long currentDateTime) {
        //    long currentDateTime = System.currentTimeMillis();
        //creating Date from millisecond
        Date currentDate = new Date(currentDateTime);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //formatted value of current Date
        return df.format(currentDate);
    }
    
	/**
	 * Returns an exception's full stack trace as a string.
	 *
	 * @param ex exception to render
	 * @return the stack trace, as {@link Exception#printStackTrace()} would print it
	 */
	public static String logException(Exception ex) {
		StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        ex.printStackTrace(pw); // Write stack trace to the PrintWriter
        return sw.toString();
	}



   
}

