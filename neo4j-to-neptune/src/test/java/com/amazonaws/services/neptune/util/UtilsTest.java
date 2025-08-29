/*
Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.util;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for Utils class
 */
public class UtilsTest {

    @Test
    public void testFormatFileSize_Bytes() {
        // Test bytes (0-1023)
        assertEquals("0 B", Utils.formatFileSize(0));
        assertEquals("1 B", Utils.formatFileSize(1));
        assertEquals("512 B", Utils.formatFileSize(512));
        assertEquals("1023 B", Utils.formatFileSize(1023));
    }

    @Test
    public void testFormatFileSize_Kilobytes() {
        // Test kilobytes (1024 - 1048575)
        assertEquals("1.0 KB", Utils.formatFileSize(1024));
        assertEquals("1.5 KB", Utils.formatFileSize(1536)); // 1024 + 512
        assertEquals("2.0 KB", Utils.formatFileSize(2048));
        assertEquals("10.5 KB", Utils.formatFileSize(10752)); // 10.5 * 1024
        assertEquals("1024.0 KB", Utils.formatFileSize(1048575)); // Just under 1MB
    }

    @Test
    public void testFormatFileSize_Megabytes() {
        // Test megabytes (1048576 - 1073741823)
        assertEquals("1.0 MB", Utils.formatFileSize(1048576)); // 1024 * 1024
        assertEquals("1.5 MB", Utils.formatFileSize(1572864)); // 1.5 * 1024 * 1024
        assertEquals("2.0 MB", Utils.formatFileSize(2097152)); // 2 * 1024 * 1024
        assertEquals("10.5 MB", Utils.formatFileSize(11010048)); // 10.5 * 1024 * 1024
        assertEquals("500.0 MB", Utils.formatFileSize(524288000)); // 500 * 1024 * 1024
        assertEquals("1024.0 MB", Utils.formatFileSize(1073741823)); // Just under 1GB
    }

    @Test
    public void testFormatFileSize_Gigabytes() {
        // Test gigabytes (1073741824 and above)
        assertEquals("1.0 GB", Utils.formatFileSize(1073741824L)); // 1024 * 1024 * 1024
        assertEquals("1.5 GB", Utils.formatFileSize(1610612736L)); // 1.5 * 1024 * 1024 * 1024
        assertEquals("2.0 GB", Utils.formatFileSize(2147483648L)); // 2 * 1024 * 1024 * 1024
        assertEquals("10.5 GB", Utils.formatFileSize(11274289152L)); // 10.5 * 1024 * 1024 * 1024
        assertEquals("100.0 GB", Utils.formatFileSize(107374182400L)); // 100 * 1024 * 1024 * 1024
        assertEquals("1000.0 GB", Utils.formatFileSize(1073741824000L)); // 1000 * 1024 * 1024 * 1024
    }

    @Test
    public void testFormatFileSize_BoundaryValues() {
        // Test exact boundary values
        assertEquals("1023 B", Utils.formatFileSize(1023));
        assertEquals("1.0 KB", Utils.formatFileSize(1024));

        assertEquals("1024.0 KB", Utils.formatFileSize(1048575)); // 1024*1024 - 1
        assertEquals("1.0 MB", Utils.formatFileSize(1048576)); // 1024*1024

        assertEquals("1024.0 MB", Utils.formatFileSize(1073741823)); // 1024*1024*1024 - 1
        assertEquals("1.0 GB", Utils.formatFileSize(1073741824)); // 1024*1024*1024
    }

    @Test
    public void testFormatFileSize_DecimalPrecision() {
        // Test decimal precision (should be 1 decimal place)
        assertEquals("1.1 KB", Utils.formatFileSize(1126)); // 1.1 * 1024
        assertEquals("1.9 KB", Utils.formatFileSize(1946)); // 1.9 * 1024
        assertEquals("1.1 MB", Utils.formatFileSize(1153434)); // 1.1 * 1024 * 1024
        assertEquals("1.9 MB", Utils.formatFileSize(1992294)); // 1.9 * 1024 * 1024
        assertEquals("1.1 GB", Utils.formatFileSize(1181116006L)); // 1.1 * 1024 * 1024 * 1024
        assertEquals("1.9 GB", Utils.formatFileSize(2040109465L)); // 1.9 * 1024 * 1024 * 1024
    }

    @Test
    public void testFormatFileSize_LargeValues() {
        // Test very large values
        assertEquals("1024.0 GB", Utils.formatFileSize(1099511627776L)); // 1TB in GB format
        assertEquals("2048.0 GB", Utils.formatFileSize(2199023255552L)); // 2TB in GB format
        assertEquals("10240.0 GB", Utils.formatFileSize(10995116277760L)); // 10TB in GB format
    }

    @Test
    public void testFormatFileSize_EdgeCases() {
        // Test edge cases
        assertEquals("0 B", Utils.formatFileSize(0));
        assertEquals("1 B", Utils.formatFileSize(1));

        // Test maximum long value (though unrealistic for file sizes)
        long maxLong = Long.MAX_VALUE;
        String result = Utils.formatFileSize(maxLong);
        assertTrue("Should format very large numbers as GB", result.endsWith(" GB"));
        assertTrue("Should be a very large number", result.startsWith("8589934592")); // ~8.6 billion GB
    }

    @Test
    public void testFormatFileSize_ConsistentFormatting() {
        // Verify consistent decimal formatting
        String result1KB = Utils.formatFileSize(1024);
        String result1MB = Utils.formatFileSize(1048576);
        String result1GB = Utils.formatFileSize(1073741824);

        assertTrue("KB should have .0 format", result1KB.contains(".0"));
        assertTrue("MB should have .0 format", result1MB.contains(".0"));
        assertTrue("GB should have .0 format", result1GB.contains(".0"));
    }

    @Test
    public void testFormatFileSize_RoundingBehavior() {
        // Test rounding behavior for edge cases
        assertEquals("1.0 KB", Utils.formatFileSize(1024)); // Exact
        assertEquals("1.0 KB", Utils.formatFileSize(1025)); // Should round to 1.0
        assertEquals("1.0 KB", Utils.formatFileSize(1075)); // Should round to 1.0 (1075/1024 = 1.05)
        assertEquals("1.1 KB", Utils.formatFileSize(1126)); // Should round to 1.1 (1126/1024 = 1.1)
        assertEquals("1.1 KB", Utils.formatFileSize(1177)); // Should round to 1.1 (1177/1024 = 1.15)
    }

    @Test
    public void testUtilsClassCannotBeInstantiated() {
        // Test that Utils class has private constructor (utility class pattern)
        try {
            // This should work since we're in the same package, but constructor should be private
            java.lang.reflect.Constructor<Utils> constructor = Utils.class.getDeclaredConstructor();
            assertTrue("Constructor should be private",
                java.lang.reflect.Modifier.isPrivate(constructor.getModifiers()));
        } catch (NoSuchMethodException e) {
            fail("Utils class should have a private no-args constructor");
        }
    }
}
