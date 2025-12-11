package org.apache.cockpit.connectors.api.jdbc.converter;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class ConverterLoader {

    public static DataTypeConverter<?> loadDataTypeConverter(String identifier) {
        return loadDataTypeConverter(identifier, Thread.currentThread().getContextClassLoader());
    }

    public static DataTypeConverter<?> loadDataTypeConverter(
            String identifier, ClassLoader classLoader) {
        List<DataTypeConverter> converters =
                discoverConverters(DataTypeConverter.class, classLoader);
        for (DataTypeConverter dataTypeConverter : converters) {
            if (dataTypeConverter.identifier().equals(identifier)) {
                return dataTypeConverter;
            }
        }
        throw new IllegalArgumentException(
                "No data type converter found for identifier: " + identifier);
    }

    public static DataConverter<?> loadDataConverter(String identifier) {
        return loadDataConverter(identifier, Thread.currentThread().getContextClassLoader());
    }

    public static DataConverter<?> loadDataConverter(String identifier, ClassLoader classLoader) {
        List<DataConverter> converters = discoverConverters(DataConverter.class, classLoader);
        for (DataConverter dataConverter : converters) {
            if (dataConverter.identifier().equals(identifier)) {
                return dataConverter;
            }
        }
        throw new IllegalArgumentException("No data converter found for identifier: " + identifier);
    }

    public static TypeConverter<?> loadTypeConverter(String identifier) {
        return loadTypeConverter(identifier, Thread.currentThread().getContextClassLoader());
    }

    public static TypeConverter<?> loadTypeConverter(String identifier, ClassLoader classLoader) {
        List<TypeConverter> converters = discoverConverters(TypeConverter.class, classLoader);
        for (TypeConverter typeConverter : converters) {
            if (typeConverter.identifier().equals(identifier)) {
                return typeConverter;
            }
        }
        throw new IllegalArgumentException("No type converter found for identifier: " + identifier);
    }

    private static <T> List<T> discoverConverters(Class<T> clazz, ClassLoader classLoader) {
        List<T> converters = new ArrayList<>();
        ServiceLoader.load(clazz, classLoader).forEach(t -> converters.add(t));
        return converters;
    }
}
