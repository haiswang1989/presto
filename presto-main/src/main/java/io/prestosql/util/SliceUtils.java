package io.prestosql.util;

import io.airlift.slice.Slice;

/**
 * @author jinhai
 * @date 2019/05/07
 */
public final class SliceUtils
{
    private SliceUtils() {}

    private static final int defaultInt = 0;
    private static final long defaultLong = 0L;
    private static final double defaultDouble = 0.0d;

    public static int toInt(final Slice slice)
    {
        String str = slice.toStringUtf8();
        try {
            return Integer.parseInt(str);
        } catch (final NumberFormatException nfe) {
            return defaultInt;
        }
    }

    public static long toLong(final Slice slice)
    {
        String str = slice.toStringUtf8();
        try {
            return Long.parseLong(str);
        } catch (final NumberFormatException nfe) {
            return defaultLong;
        }
    }

    public static double toDouble(final Slice slice)
    {
        String str = slice.toStringUtf8();
        try {
            return Double.parseDouble(str);
        } catch (final NumberFormatException nfe) {
            return defaultDouble;
        }
    }
}