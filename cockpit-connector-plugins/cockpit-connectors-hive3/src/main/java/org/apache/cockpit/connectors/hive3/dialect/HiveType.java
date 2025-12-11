package org.apache.cockpit.connectors.hive3.dialect;


import java.sql.SQLType;
import java.sql.Types;

/**
 * Hive 3.x 数据类型枚举
 * 参考: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types
 */
public enum HiveType implements SQLType {

    /**
     * VOID
     * 表示空值或无值
     */
    VOID("VOID", Types.NULL, Object.class, "", 0L, ""),

    /**
     * BOOLEAN
     * 布尔类型，true/false
     */
    BOOLEAN("BOOLEAN", Types.BOOLEAN, Boolean.class, "", 1L, ""),

    /**
     * TINYINT
     * 1字节有符号整数，范围：-128 到 127
     */
    TINYINT("TINYINT", Types.TINYINT, Byte.class, "", 3L, ""),

    /**
     * SMALLINT
     * 2字节有符号整数，范围：-32,768 到 32,767
     */
    SMALLINT("SMALLINT", Types.SMALLINT, Short.class, "", 5L, ""),

    /**
     * INT
     * 4字节有符号整数，范围：-2,147,483,648 到 2,147,483,647
     */
    INT("INT", Types.INTEGER, Integer.class, "", 10L, ""),

    /**
     * INTEGER - INT的同义词
     */
    INTEGER("INTEGER", Types.INTEGER, Integer.class, "", 10L, ""),

    /**
     * BIGINT
     * 8字节有符号整数，范围：-9,223,372,036,854,775,808 到 9,223,372,036,854,775,807
     */
    BIGINT("BIGINT", Types.BIGINT, Long.class, "", 19L, ""),

    /**
     * FLOAT
     * 4字节单精度浮点数
     */
    FLOAT("FLOAT", Types.FLOAT, Float.class, "", 7L, ""),

    /**
     * DOUBLE
     * 8字节双精度浮点数
     */
    DOUBLE("DOUBLE", Types.DOUBLE, Double.class, "", 15L, ""),

    /**
     * DECIMAL[(precision[, scale])]
     * 精确数值类型，precision表示总位数，scale表示小数位数
     * 默认：DECIMAL(10,0)
     * 最大：DECIMAL(38,38)
     */
    DECIMAL("DECIMAL", Types.DECIMAL, java.math.BigDecimal.class, "(precision[, scale])", 38L, ""),

    /**
     * STRING
     * 可变长度字符串，无长度限制
     */
    STRING("STRING", Types.VARCHAR, String.class, "", 65535L, ""),

    /**
     * VARCHAR(max_length)
     * 可变长度字符串，最大长度65,535
     */
    VARCHAR("VARCHAR", Types.VARCHAR, String.class, "(max_length)", 65535L, ""),

    /**
     * CHAR(length)
     * 固定长度字符串，最大长度255
     */
    CHAR("CHAR", Types.CHAR, String.class, "(length)", 255L, ""),

    /**
     * DATE
     * 日期类型，格式：'YYYY-MM-DD'
     */
    DATE("DATE", Types.DATE, java.sql.Date.class, "", 10L, ""),

    /**
     * TIMESTAMP
     * 时间戳类型，包含日期和时间，精确到纳秒
     */
    TIMESTAMP("TIMESTAMP", Types.TIMESTAMP, java.sql.Timestamp.class, "", 29L, ""),

    /**
     * BINARY
     * 二进制数据
     */
    BINARY("BINARY", Types.BINARY, byte[].class, "", 65535L, ""),

    /**
     * ARRAY<data_type>
     * 数组类型
     */
    ARRAY("ARRAY", Types.ARRAY, Object.class, "<data_type>", 65535L, ""),

    /**
     * MAP<primitive_type, data_type>
     * 映射类型
     */
    MAP("MAP", Types.JAVA_OBJECT, Object.class, "<primitive_type, data_type>", 65535L, ""),

    /**
     * STRUCT<col_name : data_type [COMMENT col_comment], ...>
     * 结构类型
     */
    STRUCT("STRUCT", Types.STRUCT, Object.class, "<col_name : data_type [COMMENT col_comment], ...>", 65535L, ""),

    /**
     * UNIONTYPE<data_type, data_type, ...>
     * 联合类型
     */
    UNIONTYPE("UNIONTYPE", Types.OTHER, Object.class, "<data_type, data_type, ...>", 65535L, ""),

    /**
     * INTERVAL
     * 时间间隔类型
     */
    INTERVAL("INTERVAL", Types.OTHER, Object.class, "", 0L, "");

    private final String name;
    private final int jdbcType;
    private final Class<?> javaClass;
    private final String createParams;
    private final Long precision;
    private final String description;

    private HiveType(String name, int jdbcType, Class<?> javaClass, String createParams, Long precision, String description) {
        this.name = name;
        this.jdbcType = jdbcType;
        this.javaClass = javaClass;
        this.createParams = createParams;
        this.precision = precision;
        this.description = description;
    }

    /**
     * 根据Hive类型名称获取对应的HiveType枚举
     *
     * @param fullHiveTypeName 完整的Hive类型名称，如 "DECIMAL(10,2)", "VARCHAR(100)"
     * @return 对应的HiveType枚举
     */
    public static HiveType getByName(String fullHiveTypeName) {
        if (fullHiveTypeName == null || fullHiveTypeName.trim().isEmpty()) {
            return STRING; // 默认返回STRING类型
        }

        String typeName = fullHiveTypeName.trim().toUpperCase();

        // 移除括号内的参数，只获取基本类型名称
        if (typeName.contains("(")) {
            typeName = typeName.substring(0, typeName.indexOf("(")).trim();
        }

        // 移除数组、映射等复杂类型的尖括号
        if (typeName.contains("<")) {
            typeName = typeName.substring(0, typeName.indexOf("<")).trim();
        }

        // 根据类型名称匹配枚举
        for (HiveType hiveType : values()) {
            if (hiveType.name.equalsIgnoreCase(typeName)) {
                return hiveType;
            }
        }

        // 处理一些特殊情况
        if (typeName.startsWith("ARRAY")) {
            return ARRAY;
        } else if (typeName.startsWith("MAP")) {
            return MAP;
        } else if (typeName.startsWith("STRUCT")) {
            return STRUCT;
        } else if (typeName.startsWith("UNIONTYPE")) {
            return UNIONTYPE;
        }

        return STRING; // 默认返回STRING类型
    }

    /**
     * 根据JDBC类型获取对应的HiveType枚举
     *
     * @param jdbcType JDBC类型常量
     * @return 对应的HiveType枚举
     */
    public static HiveType getByJdbcType(int jdbcType) {
        switch (jdbcType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return BOOLEAN;

            case Types.TINYINT:
                return TINYINT;

            case Types.SMALLINT:
                return SMALLINT;

            case Types.INTEGER:
                return INT;

            case Types.BIGINT:
                return BIGINT;

            case Types.FLOAT:
            case Types.REAL:
                return FLOAT;

            case Types.DOUBLE:
                return DOUBLE;

            case Types.DECIMAL:
            case Types.NUMERIC:
                return DECIMAL;

            case Types.CHAR:
                return CHAR;

            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
                return STRING;

            case Types.DATE:
                return DATE;

            case Types.TIME:
            case Types.TIMESTAMP:
                return TIMESTAMP;

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return BINARY;

            case Types.ARRAY:
                return ARRAY;

            case Types.STRUCT:
                return STRUCT;

            case Types.OTHER:
                // 可能是MAP、UNIONTYPE等复杂类型
                return STRING; // 默认返回STRING

            case Types.NULL:
                return VOID;

            default:
                return STRING; // 默认返回STRING类型
        }
    }

    /**
     * 判断是否为数字类型
     *
     * @return 如果是数字类型返回true，否则返回false
     */
    public boolean isNumeric() {
        return this == TINYINT || this == SMALLINT || this == INT || this == INTEGER ||
                this == BIGINT || this == FLOAT || this == DOUBLE || this == DECIMAL;
    }

    /**
     * 判断是否为整数类型
     *
     * @return 如果是整数类型返回true，否则返回false
     */
    public boolean isInteger() {
        return this == TINYINT || this == SMALLINT || this == INT || this == INTEGER || this == BIGINT;
    }

    /**
     * 判断是否为浮点类型
     *
     * @return 如果是浮点类型返回true，否则返回false
     */
    public boolean isFloatingPoint() {
        return this == FLOAT || this == DOUBLE;
    }

    /**
     * 判断是否为字符串类型
     *
     * @return 如果是字符串类型返回true，否则返回false
     */
    public boolean isString() {
        return this == STRING || this == VARCHAR || this == CHAR;
    }

    /**
     * 判断是否为日期时间类型
     *
     * @return 如果是日期时间类型返回true，否则返回false
     */
    public boolean isDateTime() {
        return this == DATE || this == TIMESTAMP;
    }

    /**
     * 判断是否为二进制类型
     *
     * @return 如果是二进制类型返回true，否则返回false
     */
    public boolean isBinary() {
        return this == BINARY;
    }

    /**
     * 判断是否为复杂类型（数组、映射、结构体、联合类型）
     *
     * @return 如果是复杂类型返回true，否则返回false
     */
    public boolean isComplex() {
        return this == ARRAY || this == MAP || this == STRUCT || this == UNIONTYPE;
    }

    /**
     * 获取类型的默认精度
     *
     * @return 类型的默认精度
     */
    public Long getDefaultPrecision() {
        return this.precision;
    }

    /**
     * 获取类型的Java类
     *
     * @return 对应的Java类
     */
    public Class<?> getJavaClass() {
        return this.javaClass;
    }

    /**
     * 获取创建参数
     *
     * @return 创建参数
     */
    public String getCreateParams() {
        return this.createParams;
    }

    /**
     * 获取类型描述
     *
     * @return 类型描述
     */
    public String getDescription() {
        return this.description;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getVendor() {
        return "org.apache.hive";
    }

    @Override
    public Integer getVendorTypeNumber() {
        return this.jdbcType;
    }

    /**
     * 获取JDBC类型
     *
     * @return JDBC类型常量
     */
    public int getJdbcType() {
        return this.jdbcType;
    }

    /**
     * 根据类型名称和参数构建完整的类型字符串
     *
     * @param precision 精度（对于DECIMAL）
     * @param scale     范围（对于DECIMAL）
     * @param length    长度（对于CHAR/VARCHAR）
     * @return 完整的类型字符串
     */
    public String buildFullTypeName(Integer precision, Integer scale, Integer length) {
        switch (this) {
            case DECIMAL:
                if (precision != null && scale != null) {
                    return String.format("DECIMAL(%d,%d)", precision, scale);
                } else if (precision != null) {
                    return String.format("DECIMAL(%d)", precision);
                }
                return "DECIMAL";

            case VARCHAR:
                if (length != null && length > 0) {
                    return String.format("VARCHAR(%d)", length);
                }
                return "VARCHAR";

            case CHAR:
                if (length != null && length > 0) {
                    return String.format("CHAR(%d)", length);
                }
                return "CHAR";

            default:
                return this.name;
        }
    }

    /**
     * 从完整的类型字符串中解析精度和范围
     *
     * @param fullTypeName 完整的类型字符串，如 "DECIMAL(10,2)"
     * @return 包含精度和范围的数组，[精度, 范围]，如果没有则返回null
     */
    public static int[] parsePrecisionAndScale(String fullTypeName) {
        if (fullTypeName == null || !fullTypeName.contains("(")) {
            return null;
        }

        try {
            String params = fullTypeName.substring(fullTypeName.indexOf("(") + 1, fullTypeName.lastIndexOf(")"));
            String[] parts = params.split(",");

            if (parts.length >= 2) {
                return new int[]{
                        Integer.parseInt(parts[0].trim()),
                        Integer.parseInt(parts[1].trim())
                };
            } else if (parts.length == 1) {
                return new int[]{
                        Integer.parseInt(parts[0].trim()),
                        0
                };
            }
        } catch (Exception e) {
            // 解析失败，返回null
        }

        return null;
    }

    /**
     * 从完整的类型字符串中解析长度
     *
     * @param fullTypeName 完整的类型字符串，如 "VARCHAR(100)"
     * @return 长度，如果没有则返回null
     */
    public static Integer parseLength(String fullTypeName) {
        if (fullTypeName == null || !fullTypeName.contains("(")) {
            return null;
        }

        try {
            String params = fullTypeName.substring(fullTypeName.indexOf("(") + 1, fullTypeName.lastIndexOf(")"));
            return Integer.parseInt(params.trim());
        } catch (Exception e) {
            // 解析失败，返回null
        }

        return null;
    }

    @Override
    public String toString() {
        return this.name;
    }
}
