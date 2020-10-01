package com.baidu.hugegraph.backend.store;

import java.net.MalformedURLException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class BackendUrl {
    final String schema;
    final String host;
    final int port;
    final String path;
    final LinkedHashMap<String, Object> parameters;

    private final String text;
    private final int length;
    private int pos;

    /**
     * To adapt JDBC URL which not complies with standard URL format
     *
     * @param input       configured JDBC URL text
     * @param defaultPort default port if not specified in configuration
     * @throws MalformedURLException if parse failed
     */
    public BackendUrl(String input, int defaultPort) throws MalformedURLException {
        text = input;
        length = input.length();
        pos = 0;
        schema = parseSchema();
        host = parseHost();
        port = parsePort(defaultPort);
        path = parsePath();
        parameters = parseParameters();
    }

    private String parseSchema() throws MalformedURLException {
        int start = skipWhiteSpace();
        char ch = 0;
        for (; pos < length; pos++) {
            ch = text.charAt(pos);
            if (Character.isLetterOrDigit(ch) || ch == ':') {
                continue;
            }
            if (ch == '/') {
                while (pos < length && text.charAt(pos) == '/') {
                    pos++;
                }
            } else {
                pos++;
            }
            break;
        }
        if (start < pos) {
            return text.substring(start, pos);
        }
        throw new MalformedURLException("Invalid schema");
    }

    private String parseHost() throws MalformedURLException {
        int start = skipWhiteSpace();
        char ch = 0;
        for (; pos < length; pos++) {
            ch = text.charAt(pos);
            if (ch == '?' || ch == '/') {
                break;
            }
        }
        for (int i = pos - 1; i >= start; i--) {
            if (text.charAt(i) == ':') {
                pos = i;
                break;
            }
        }
        String temp = text.substring(start, pos).trim();
        if (temp.length() == 0) {
            throw new MalformedURLException("Invalid host");
        }
        return temp;
    }

    private int parsePort(int defaultPort) throws MalformedURLException {
        if (pos == length) {
            return defaultPort;
        }
        char ch = text.charAt(pos);
        if (ch == '?' || ch == '/') {
            return defaultPort;
        }
        if (ch != ':') {
            throw new MalformedURLException("Invalid port");
        }
        pos++;
        int start = skipWhiteSpace();
        for (; pos < length; pos++) {
            ch = text.charAt(pos);
            if (ch == '?' || ch == '/') {
                break;
            }
        }
        try {
            return Integer.valueOf(text.substring(start, pos));
        } catch (NumberFormatException e) {
            throw new MalformedURLException("Invalid port");
        }
    }

    private String parsePath() throws MalformedURLException {
        if (pos == length) {
            return "";
        }
        char ch = text.charAt(pos);
        if (ch == '?') {
            return "";
        }
        int start = pos;
        for (; pos < length; pos++) {
            ch = text.charAt(pos);
            if (ch == '?') {
                break;
            }
        }
        return text.substring(start, pos);
    }

    private LinkedHashMap<String, Object> parseParameters() throws MalformedURLException {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        if (pos == length) {
            return map;
        }
        char ch = text.charAt(pos);
        if (ch != '?') {
            throw new MalformedURLException("Invalid parameter");
        }
        pos++;
        while (pos < length) {
            while (text.charAt(pos) == '&') {
                pos++;
            }
            parseParameter(map);
        }
        return map;
    }

    private void parseParameter(LinkedHashMap<String, Object> map) throws MalformedURLException {
        int start = skipWhiteSpace();
        if (pos == length) {
            return;
        }
        char ch = 0;
        for (; pos < length; pos++) {
            ch = text.charAt(pos);
            if (ch == '=' || ch == '&') {
                break;
            }
        }
        String key = text.substring(start, pos).trim();
        if (key.length() == 0) {
            throw new MalformedURLException("Invalid parameter");
        }
        if (ch == '&') {
            map.put(key, "");
            return;
        }
        pos++;
        start = skipWhiteSpace();
        for (; pos < length; pos++) {
            ch = text.charAt(pos);
            if (ch == '&') {
                break;
            }
        }
        String value = text.substring(start, pos).trim();
        map.put(key, value);
    }


    private int skipWhiteSpace() {
        while (pos < length && Character.isWhitespace(text.charAt(pos))) {
            pos++;
        }
        return pos;
    }

    @Override
    public String toString() {
        return build(path, parameters);
    }

    /**
     * Ensure has parameter, if not, add with default value
     *
     * @param parameterKey parameter key
     * @param defaultValue default value if add
     */
    public void ensureHasParameter(String parameterKey, Object defaultValue){
        if(!parameters.containsKey(parameterKey)){
            parameters.put(parameterKey,defaultValue);
        }
    }

    /**
     * Build new URL with new path and new parameters
     *
     * @param newPath path in new URL
     * @param newParameters all parameters in new URL
     * @return new URL in text
     */
    public String build(String newPath, Map<String, Object> newParameters) {
        StringBuilder builder = new StringBuilder(128);
        builder.append(schema).append(host).append(":").append(port).append(newPath);
        Iterator<Map.Entry<String, Object>> iterator = newParameters.entrySet().iterator();
        if (iterator.hasNext()) {
            builder.append('?');
            appendParameter(builder, iterator.next());
        }
        while (iterator.hasNext()) {
            builder.append('&');
            appendParameter(builder, iterator.next());
        }
        return builder.toString();
    }

    private void appendParameter(StringBuilder builder, Map.Entry<String, Object> entry) {
        builder.append(entry.getKey()).append('=');
        Object value = entry.getValue();
        if (value != null) {
            builder.append(value);
        }
    }
}
