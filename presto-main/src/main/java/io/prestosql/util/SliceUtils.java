/*
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
        }
        catch (final NumberFormatException nfe) {
            return defaultInt;
        }
    }

    public static long toLong(final Slice slice)
    {
        String str = slice.toStringUtf8();
        try {
            return Long.parseLong(str);
        }
        catch (final NumberFormatException nfe) {
            return defaultLong;
        }
    }

    public static double toDouble(final Slice slice)
    {
        String str = slice.toStringUtf8();
        try {
            return Double.parseDouble(str);
        }
        catch (final NumberFormatException nfe) {
            return defaultDouble;
        }
    }
}
