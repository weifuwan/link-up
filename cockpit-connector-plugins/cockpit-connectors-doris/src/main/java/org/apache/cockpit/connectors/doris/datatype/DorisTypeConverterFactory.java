package org.apache.cockpit.connectors.doris.datatype;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.jdbc.converter.BasicTypeDefine;
import org.apache.cockpit.connectors.api.jdbc.converter.TypeConverter;

import java.util.Locale;

@Slf4j
public class DorisTypeConverterFactory {
    public static TypeConverter<BasicTypeDefine> getTypeConverter(@NonNull String dorisVersion) {
        if (dorisVersion.toLowerCase(Locale.ROOT).startsWith("doris version doris-1.")
                || dorisVersion.toLowerCase(Locale.ROOT).startsWith("selectdb-doris-1.")) {
            return DorisTypeConverterV1.INSTANCE;
        } else if (dorisVersion.toLowerCase(Locale.ROOT).startsWith("doris version doris-2.")
                || dorisVersion.toLowerCase(Locale.ROOT).startsWith("selectdb-doris-2.")) {
            return DorisTypeConverterV2.INSTANCE;
        } else {
            return DorisTypeConverterV2.INSTANCE;
        }
    }
}
