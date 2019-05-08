package io.prestosql.operator.scalar;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import javax.annotation.Nullable;
import java.net.URL;

import static io.airlift.slice.Slices.utf8Slice;

/**
 * @author jinhai
 * @date 2019/05/08
 */
public final class ParseUrlFunction
{
    private ParseUrlFunction() {}

    @SqlNullable
    @ScalarFunction("parse_url")
    @Description("Extracts a part from a URL")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice parseUrl(
            @SqlType(StandardTypes.VARCHAR) Slice urlStr,
            @SqlType(StandardTypes.VARCHAR) Slice partToExtract)
    {
        URL url = parseUrl(urlStr);
        if (url == null) {
            return null;
        }

        String part = partToExtract.toStringUtf8();
        switch (part) {
            case "HOST":
                return utf8Slice(url.getHost());
            case "PATH":
                return utf8Slice(url.getPath());
            case "QUERY":
                return utf8Slice(url.getQuery());
            case "REF":
                return utf8Slice(url.getRef());
            case "PROTOCOL":
                return utf8Slice(url.getProtocol());
            case "FILE":
                return utf8Slice(url.getFile());
            case "AUTHORITY":
                return utf8Slice(url.getAuthority());
            case "USERINFO":
                return utf8Slice(url.getUserInfo());
        }

        return null;
    }

    @Nullable
    private static URL parseUrl(Slice url)
    {
        try {
            return new URL(url.toStringUtf8());
        }
        catch (Exception e) {
            return null;
        }
    }
}