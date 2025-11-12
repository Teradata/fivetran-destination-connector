package com.teradata.fivetran.destination.writers.util;

import java.util.regex.Pattern;

public class TeradataColumnDesc {
    public static final int KBYTES = 1024;
    public static final int TIME_SCALE_DEFAULT = 6;
    public static final int TIME_SCALE_MAX = 6;
    public static final int DECIMAL_PRECISION_DEFAULT = 5;
    public static final int INTERVAL_N_DEFAULT = 2;
    public static final int INTERVAL_M_DEFAULT = 6;
    public static final int BYTE_DEFAULT = 1;
    public static final int CHAR_DEFAULT = 1;
    /*
     * Number data type Processing
     */
    public static final int NUMBER_PRECISION_MAX = 38;
    public static final int NUMBER_SCALE_DEFAULT = 0;
    public static final int UNBOUNDED_NUMBER_SCALE_DEFAULT = 10;
    public static final String NUMBER_DATA_TYPE = "NUMBER";
    /* Reserved for Future Use */
    public static final int DECIMAL_SCALE_DEFAULT = 0;
    public static final int INTERVAL_N_MAX = 4;
    public static final int INTERVAL_M_MAX = 6;

    /* INTERVAL Processing */
    public static final String INTERVAL_NM_SEPARATOR = "TO";
    public static final String INTERVAL_NM_SECOND = "SECOND";

    private String name = "";
    private String typeName = "";
    /*
     * class name is the fully qualified name for UDT
     */
    private String className = "";
    /*
     * for date, time, timestamp support
     */
    private String format = "";

    private int type = java.sql.Types.NULL;
    private int precision = 0;
    private int scale = 0;
    private long length = 0;
    private int timeScale = -1;
    private boolean nullable = true;
    private int charType = 0;
    private boolean caseSensitive = false;

    private static final String PATTERN_CALENDAR_TIME_TYPE = "\\s*time\\s*with\\s*time\\s*zone\\s*";
    private static final String PATTERN_CALENDAR_TIMESTAMP_TYPE = "\\s*timestamp\\s*with\\s*time\\s*zone\\s*";

    public TeradataColumnDesc() {
    }

    public TeradataColumnDesc( TeradataColumnDesc inputColumn ) {
        this.name = inputColumn.getName();
        this.typeName = inputColumn.getTypeName();
        this.className = inputColumn.getClassName();
        this.format = inputColumn.getFormat();
        this.type = inputColumn.getType();
        this.precision = inputColumn.getPrecision();
        this.scale = inputColumn.getScale();
        this.length = inputColumn.getLength();
        this.timeScale = inputColumn.getScale();
        this.nullable = inputColumn.isNullable();
        this.charType = inputColumn.getCharType();
        this.caseSensitive = inputColumn.isCaseSensitive();
    }

    public void setName(String name_) {
        name = name_;
    }

    public void setTypeName(String typeName_) {
        typeName = typeName_.toUpperCase();
    }

    public void setClassName(String className_) {
        className = className_;
    }

    public void setFormat(String format_) {
        format = format_;
    }

    public void setType(int type_) {
        type = type_;
    }

    public void setPrecision(int precision_) {
        if (precision_ >= 0)
            precision = precision_;
    }

    public void setScale(int scale_) {
        if (scale_ >= 0) {
            scale = scale_;
            timeScale = scale_;
        }
    }

    public void setLength(long length_) {
        if (length_ >= 0)
            length = length_;
    }

    public void setNullable(boolean nullable_) {
        nullable = nullable_;
    }

    public void setCaseSensitive(boolean caseSensitive_) {
        caseSensitive = caseSensitive_;
    }

    public String getName() {
        return name;
    }

    public String getTypeName() {
        return typeName;
    }

    public String getClassName() {
        return className;
    }

    public String getFormat() {
        return format;
    }

    public int getType() {
        return type;
    }

    public int getPrecision() {
        switch (type) {
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
                return (precision == 0) ? DECIMAL_PRECISION_DEFAULT : precision;
            default:
                return precision;
        }
    }

    public int getScale() {
        switch (type) {
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:
                if (timeScale < 0 || timeScale > TIME_SCALE_MAX)
                    return TIME_SCALE_DEFAULT;
                else {
                    return timeScale;
                }
            case java.sql.Types.STRUCT:
                if (typeName.contains("PERIOD")) {
                    if (typeName.contains("TIME")) {
                        if (timeScale < 0 || timeScale > TIME_SCALE_MAX)
                            return TIME_SCALE_DEFAULT;
                        else {
                            return timeScale;
                        }
                    }
                }
            default:
        }

        return scale;
    }

    public long getLength() {
        switch (type) {
            case java.sql.Types.CHAR:
                return (length == 0) ? CHAR_DEFAULT : length;
            case java.sql.Types.BINARY:
                return (length == 0) ? BYTE_DEFAULT : length;
            default:
                return length;
        }
    }

    public boolean isNullable() {
        return nullable;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public void setCharType(int charType) {
        this.charType = charType;
    }

    public int getCharType() {
        return charType;
    }

    public String getLobLengthInKMG() {
        String unit = "";

        long len = length;

        if (len > KBYTES && len % KBYTES == 0) {
            len = len / KBYTES;
            unit = "K";
        }

        if (len > KBYTES && len % KBYTES == 0) {
            len = len / KBYTES;
            unit = "M";
        }

        if (len > KBYTES && len % KBYTES == 0) {
            len = len / KBYTES;
            unit = "G";
        }
        return "" + len + unit;
    }

    public String getTypeString() {
        String string = getTypeStringWithoutNullability();

        string += nullable ? " NULL" : " NOT NULL";
        return string;
    }

    public static boolean isTimeWithTimeZoneType(String typeName) {
        return Pattern.matches(PATTERN_CALENDAR_TIME_TYPE,
                typeName.toLowerCase());
    }

    public static boolean isTimestampWithTimeZoneType(String typeName) {
        return Pattern.matches(PATTERN_CALENDAR_TIMESTAMP_TYPE,
                typeName.toLowerCase());
    }


    public String getTypeString4Using(String charset, int timeScale, int timeStampScale) {
        int strCharOrVarcharMultiplier;
        int strTimeOrIntervalMultiplier;
        int timeZoneLen = 0;

        switch (type) {
            case java.sql.Types.NUMERIC:
                if((precision == 40) && (length == 47) && (scale == 0)) {
                    scale = UNBOUNDED_NUMBER_SCALE_DEFAULT;
                }
                return "DECIMAL (38, " + scale + ")";
            case java.sql.Types.DECIMAL:
                return "DECIMAL (38, " + scale + ")";
            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.OTHER:
                strCharOrVarcharMultiplier = 1;
                if ("UTF16".equalsIgnoreCase(charset)) {
                    strCharOrVarcharMultiplier = 2;
                } else if ("UTF8".equalsIgnoreCase(charset)) {
                    strCharOrVarcharMultiplier = 3;
                    /*
                     * Note that strTimeOrIntervalMultiplier remains at 1!
                     */
                }
                return "VARCHAR(" + strCharOrVarcharMultiplier * getLength() + ")";
            case java.sql.Types.CLOB:
                return "CLOB(" + getLobLengthInKMG() + ")";
            case java.sql.Types.TIME:
                strTimeOrIntervalMultiplier = 1;
                if ("UTF16".equalsIgnoreCase(charset)) {
                    strTimeOrIntervalMultiplier = 2;
                }
                int tnanoLength = timeScale > 0 ? 8 + timeScale + 1 : 8;

                if (isTimeWithTimeZoneType(typeName)) {
                    timeZoneLen = 6;
                }
                return "CHAR(" + (strTimeOrIntervalMultiplier * (tnanoLength + timeZoneLen)) + ")";
            case java.sql.Types.TIMESTAMP:
                strTimeOrIntervalMultiplier = 1;
                if ("UTF16".equalsIgnoreCase(charset)) {
                    strTimeOrIntervalMultiplier = 2;
                }
                int tsnanoLength = timeStampScale > 0 ? 19 + timeStampScale + 1 : 19;
                if (isTimestampWithTimeZoneType(typeName)) {
                    timeZoneLen = 6;
                }
                return "CHAR(" + (strTimeOrIntervalMultiplier * (tsnanoLength + timeZoneLen)) + ")";
            case java.sql.Types.NULL:
                return getTypeStringWithoutNullability();
            default:
                return getTypeStringWithoutNullability();
        }
    }

    public String getTypeStringWithoutNullability() {
        String string = "";

        switch (type) {
            /* CHAR related types */
            case java.sql.Types.CHAR: {
                string += "CHAR(" + length + ")";
                if (charType == 1) {
                    string += " CHARACTER SET LATIN";
                } else if (charType == 2) {
                    string += " CHARACTER SET UNICODE";
                }

                if (caseSensitive) {
                    string += " CASESPECIFIC";
                } else {
                    string += " NOT CASESPECIFIC";
                }
                break;
            }
            case java.sql.Types.VARCHAR: {
                string += "VARCHAR(" + length + ")";
                if (charType == 1) {
                    string += " CHARACTER SET LATIN";
                } else if (charType == 2) {
                    string += " CHARACTER SET UNICODE";
                }

                if (caseSensitive) {
                    string += " CASESPECIFIC";
                } else {
                    string += " NOT CASESPECIFIC";
                }
                break;
            }
            case java.sql.Types.LONGVARCHAR: {
                string += "LONG VARCHAR";
                if (charType == 1) {
                    string += " CHARACTER SET LATIN";
                } else if (charType == 2) {
                    string += " CHARACTER SET UNICODE";
                }

                if (caseSensitive) {
                    string += " CASESPECIFIC";
                } else {
                    string += " NOT CASESPECIFIC";
                }
                break;
            }
            /* INT related types */
            case java.sql.Types.INTEGER:
                string += "INTEGER";
                break;
            case java.sql.Types.SMALLINT:
                string += "SMALLINT";
                break;
            case java.sql.Types.BIGINT:
                string += "BIGINT";
                break;
            case java.sql.Types.TINYINT:
                string += "BYTEINT";
                break;

            /* FLOAT related types */
            case java.sql.Types.FLOAT:
                string += "FLOAT";
                break;
            case java.sql.Types.REAL:
                string += "REAL";
                break;
            case java.sql.Types.DOUBLE:
                string += "DOUBLE PRECISION";
                break;
            /* DECIMAL related types */
            case java.sql.Types.DECIMAL:
                string += "DECIMAL(" + precision + ", " + scale + ")";
                break;
            case java.sql.Types.NUMERIC:
                if (typeName.equalsIgnoreCase(NUMBER_DATA_TYPE)) {
                    if (precision > NUMBER_PRECISION_MAX) {
                        if (scale > NUMBER_SCALE_DEFAULT) {
                            string += "NUMBER(*, " + scale + ")";
                        } else {
                            string += "NUMBER(*)";
                        }
                    } else {
                        string += "NUMBER(" + precision + ", " + scale + ")";
                    }
                } else {
                    string += "NUMERIC(" + precision + ", " + scale + ")";
                }
                break;

            /* BYTE related types */
            case java.sql.Types.BINARY:
                string += "BYTE(" + length + ")";
                break;
            case java.sql.Types.VARBINARY:
                string += "VARBYTE(" + length + ")";
                break;

            /* DATE TIME related types */
            case java.sql.Types.DATE:
                string += "DATE";
                break;
            case java.sql.Types.TIME:
                string += "TIME";
                if (scale >= 0 && scale < TeradataColumnDesc.TIME_SCALE_MAX)
                    string += "(" + scale + ")";
                if (isTimeWithTimeZoneType(typeName))
                    string += " WITH TIME ZONE "; //TDCH-234: support time zone.
                if (!isEmptyString(format))
                    string += " FORMAT '" + format + "'";
                break;
            case java.sql.Types.TIMESTAMP:
                string += "TIMESTAMP";
                if (scale >= 0 && scale < TeradataColumnDesc.TIME_SCALE_MAX)
                    string += "(" + scale + ")";
                if (isTimestampWithTimeZoneType(typeName))
                    string += " WITH TIME ZONE "; //TDCH-234: support time zone.
                if (!isEmptyString(format))
                    string += " FORMAT '" + format + "'";
                break;

            /* LOB related types */
            case java.sql.Types.BLOB:
                string += "BLOB(" + getLobLengthInKMG() + ")";
                break;
            case java.sql.Types.CLOB: {
                string += "CLOB(" + getLobLengthInKMG() + ")";
                if (charType == 1) {
                    string += " CHARACTER SET LATIN";
                } else if (charType == 2) {
                    string += " CHARACTER SET UNICODE";
                }
                break;
            }
            /* PERIOD related type */
            case java.sql.Types.STRUCT:
                if (typeName.contains("PERIOD")) {
                    if (typeName.contains("TIMESTAMP")) {
                        string += "PERIOD(TIMESTAMP";
                        if (scale >= 0 && scale < TeradataColumnDesc.TIME_SCALE_MAX)
                            string += "(" + scale + ")";
                        string += ")";
                    } else if (typeName.contains("TIME")) {
                        string += "PERIOD(TIME";
                        if (scale >= 0 && scale < TeradataColumnDesc.TIME_SCALE_MAX)
                            string += "(" + scale + ")";
                        string += ")";
                    } else if (typeName.contains("DATE")) {
                        string += "PERIOD(DATE)";
                    } else
                        string += typeName;
                } else {
                    string += typeName;
                }
                break;

            /* INTERVAL related type */
            case java.sql.Types.OTHER:
                if (typeName.contains("INTERVAL")) {
                    int index = typeName.indexOf(TeradataColumnDesc.INTERVAL_NM_SEPARATOR);
                    if (index >= 0) {
                        String typeN = typeName.substring(0, index);
                        String typeM = typeName.substring(index + TeradataColumnDesc.INTERVAL_NM_SEPARATOR.length());

                        int n = TeradataColumnDesc.INTERVAL_N_DEFAULT;
                        int m = TeradataColumnDesc.INTERVAL_M_DEFAULT;

                        String formatString = format.trim();

                        int pos = formatString.length();

                        if (typeM.contains(TeradataColumnDesc.INTERVAL_NM_SECOND)) {
                            if (pos > 3 && formatString.charAt(pos - 1) == ')') {
                                try {
                                    m = Integer.parseInt("" + formatString.charAt(pos - 2));
                                    formatString = formatString.substring(0, pos - 3);
                                } catch (NumberFormatException e) {
                                    m = TeradataColumnDesc.INTERVAL_M_DEFAULT;
                                }
                            }
                        }

                        pos = formatString.indexOf('(');
                        if (pos > 0) {
                            try {
                                n = Integer.parseInt("" + formatString.charAt(pos + 1));
                            } catch (NumberFormatException e) {
                                n = TeradataColumnDesc.INTERVAL_N_DEFAULT;
                            }
                        }

                        string += typeN;
                        if (n != TeradataColumnDesc.INTERVAL_N_DEFAULT)
                            string += "(" + n + ") ";

                        string += TeradataColumnDesc.INTERVAL_NM_SEPARATOR + typeM;

                        if (m != TeradataColumnDesc.INTERVAL_M_DEFAULT)
                            string += " (" + m + ")";

                    } else {
                        int n = TeradataColumnDesc.INTERVAL_N_DEFAULT;
                        String formatString = format.trim();
                        int pos = formatString.indexOf('(');

                        if (pos > 0) {
                            try {
                                n = Integer.parseInt("" + formatString.charAt(pos + 1));
                            } catch (NumberFormatException e) {
                                n = TeradataColumnDesc.INTERVAL_N_DEFAULT;
                            }
                        }

                        string += typeName;
                        if (n != TeradataColumnDesc.INTERVAL_N_DEFAULT)
                            string += "(" + n + ")";
                    }
                }
                break;

            /*
             * Teradata Array
             */
            case java.sql.Types.ARRAY:
                string += typeName;
                break;

            /*
             * Unsupported by Teradata JDBC Driver
             */
            case java.sql.Types.BIT:
            case java.sql.Types.BOOLEAN:
            case java.sql.Types.JAVA_OBJECT:
            case java.sql.Types.REF:
            case java.sql.Types.LONGVARBINARY:
            default:
                string += typeName;
                break;
        }

        return string;
    }

    /* General Utility Functions */
    protected boolean isEmptyString(String text) {
        return text == null || text.isEmpty();
    }
}