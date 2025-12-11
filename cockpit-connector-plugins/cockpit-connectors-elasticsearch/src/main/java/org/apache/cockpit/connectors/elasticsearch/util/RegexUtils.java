package org.apache.cockpit.connectors.elasticsearch.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexUtils {

    public static List<String> extractDatas(String content, String regex) {
        List<String> datas = new ArrayList<>();
        Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
        Matcher matcher = pattern.matcher(content);
        while (matcher.find()) {
            String result = matcher.group(1);
            datas.add(result);
        }
        return datas;
    }
}
