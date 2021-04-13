package com.sensetime.bigdata.tool;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Map;

public class CustomGsonKit {

    private static Logger log = LoggerFactory.getLogger(CustomGsonKit.class);

    /**
     * Json 嵌套字段扁平化
     * <p>
     * { "field_a": {"nested_field_b": "c"} } -> { "field_a.nested_field_b": "c" }
     *
     * @param jsonObj
     * @return
     */
    public static JsonObject toFlattenJson(JsonObject jsonObj) {
        return recursiveJsonFlatten(jsonObj, new JsonObject(), "");
    }

    public static JsonObject toFlattenJson(String jsonStr) {
        JsonObject jsonObject = new JsonParser().parse(jsonStr).getAsJsonObject();
        return toFlattenJson(jsonObject);
    }


    /**
     * 递归检测嵌套的 Json，并将其扁平化
     *
     * @param object    待检测的 JsonObject
     * @param flattened 扁平化后的 JsonObject
     * @param father
     * @return
     */
    private static JsonObject recursiveJsonFlatten(JsonObject object, JsonObject flattened, String father) {
        for (Map.Entry entry : object.entrySet()) {
            String midFather = entry.getKey().toString();
            // 父级 key 名称
            String tmp = father;
            JsonElement tmpVa = (JsonElement) entry.getValue();
            try {
                // 检测到多层json的时候进行递归处理
                if (tmpVa.isJsonObject()) {
                    // 当前层键与之前外层键进行拼接
                    tmp = tmp + "." + midFather;
                    recursiveJsonFlatten(object.getAsJsonObject(entry.getKey().toString()), flattened, tmp);
                } else {
                    // 当前层的值没有嵌套 json 键值对，将键值对添加到 flattened 中
                    String nowKeyTmp = father + "." + entry.getKey().toString();
                    String nowKey = nowKeyTmp.substring(1);
                    flattened.add(nowKey, ((JsonElement) entry.getValue()));
                }
            } catch (JsonIOException e) {
                e.printStackTrace();
                log.error("", e);
            }
        }
        return flattened;
    }

    public static GsonBuilder newCustomGsonBuilder() {
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(LocalDateTime.class, new LocalDateTimeSerializer());
        builder.registerTypeAdapter(LocalDateTime.class, new LocalDateTimeDeserializer());
        // Gson 默认会进行 HTML ESCAPE（转码），禁用，否则 = 会被转换成 \u003d
        builder.disableHtmlEscaping();
        return builder;
    }

    /**
     * {@link JsonArray} 转 {@link String[]}
     *
     * @param jsonArr
     * @return
     */
    public static String[] toStringArray(JsonArray jsonArr) {
        return newCustomGsonBuilder().create().fromJson(
                jsonArr, new TypeToken<String[]>() {}.getType());
    }
}
