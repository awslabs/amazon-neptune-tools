package com.amazonaws.services.neptune.util;

import lombok.RequiredArgsConstructor;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@RequiredArgsConstructor
public class NeptuneEngineVersionUtilTest {
    private final String engineVersion;
    private final NeptuneEngineVersionUtil.NeptuneEngineVersion expectedNeptuneEngineVersion;
    private final NeptuneEngineVersionUtil.NeptuneDBParameterGroupFamily expectedNeptuneDBParameterGroupFamily;
    private final NeptuneEngineVersionUtil.NeptuneClusterParameterGroupFamily expectedNeptuneClusterParameterGroupFamily;

    @Parameterized.Parameters(name = "engineVersion = {0}")
    public static Object[][] parameters() {
        return new Object[][]{
                new Object[]{"1.0.1.0",
                        NeptuneEngineVersionUtil.NeptuneEngineVersion.builder()
                                .majorVersion("1.0")
                                .minorVersion("1.0")
                                .build(),
                        NeptuneEngineVersionUtil.NeptuneDBParameterGroupFamily.NEPTUNE1,
                        NeptuneEngineVersionUtil.NeptuneClusterParameterGroupFamily.NEPTUNE1},
                new Object[]{"1.0.2.0",
                        NeptuneEngineVersionUtil.NeptuneEngineVersion.builder()
                                .majorVersion("1.0")
                                .minorVersion("2.0")
                                .build(),
                        NeptuneEngineVersionUtil.NeptuneDBParameterGroupFamily.NEPTUNE1,
                        NeptuneEngineVersionUtil.NeptuneClusterParameterGroupFamily.NEPTUNE1},
                new Object[]{"1.0.2.1",
                        NeptuneEngineVersionUtil.NeptuneEngineVersion.builder()
                                .majorVersion("1.0")
                                .minorVersion("2.1")
                                .build(),
                        NeptuneEngineVersionUtil.NeptuneDBParameterGroupFamily.NEPTUNE1,
                        NeptuneEngineVersionUtil.NeptuneClusterParameterGroupFamily.NEPTUNE1},
                new Object[]{"1.0.3.0",
                        NeptuneEngineVersionUtil.NeptuneEngineVersion.builder()
                                .majorVersion("1.0")
                                .minorVersion("3.0")
                                .build(),
                        NeptuneEngineVersionUtil.NeptuneDBParameterGroupFamily.NEPTUNE1,
                        NeptuneEngineVersionUtil.NeptuneClusterParameterGroupFamily.NEPTUNE1},
                new Object[]{"1.0.4.0",
                        NeptuneEngineVersionUtil.NeptuneEngineVersion.builder()
                                .majorVersion("1.0")
                                .minorVersion("4.0")
                                .build(),
                        NeptuneEngineVersionUtil.NeptuneDBParameterGroupFamily.NEPTUNE1,
                        NeptuneEngineVersionUtil.NeptuneClusterParameterGroupFamily.NEPTUNE1},
                new Object[]{"1.0.5.0",
                        NeptuneEngineVersionUtil.NeptuneEngineVersion.builder()
                                .majorVersion("1.0")
                                .minorVersion("5.0")
                                .build(),
                        NeptuneEngineVersionUtil.NeptuneDBParameterGroupFamily.NEPTUNE1,
                        NeptuneEngineVersionUtil.NeptuneClusterParameterGroupFamily.NEPTUNE1},
                new Object[]{"1.1.0.0",
                        NeptuneEngineVersionUtil.NeptuneEngineVersion.builder()
                                .majorVersion("1.1")
                                .minorVersion("0.0")
                                .build(),
                        NeptuneEngineVersionUtil.NeptuneDBParameterGroupFamily.NEPTUNE1,
                        NeptuneEngineVersionUtil.NeptuneClusterParameterGroupFamily.NEPTUNE1},
                new Object[]{"1.1.1.0",
                        NeptuneEngineVersionUtil.NeptuneEngineVersion.builder()
                                .majorVersion("1.1")
                                .minorVersion("1.0")
                                .build(),
                        NeptuneEngineVersionUtil.NeptuneDBParameterGroupFamily.NEPTUNE1,
                        NeptuneEngineVersionUtil.NeptuneClusterParameterGroupFamily.NEPTUNE1},
                new Object[]{"1.2.0.0",
                        NeptuneEngineVersionUtil.NeptuneEngineVersion.builder()
                                .majorVersion("1.2")
                                .minorVersion("0.0")
                                .build(),
                        NeptuneEngineVersionUtil.NeptuneDBParameterGroupFamily.NEPTUNE1_2,
                        NeptuneEngineVersionUtil.NeptuneClusterParameterGroupFamily.NEPTUNE1_2},
                new Object[]{"1.2.0.1",
                        NeptuneEngineVersionUtil.NeptuneEngineVersion.builder()
                                .majorVersion("1.2")
                                .minorVersion("0.1")
                                .build(),
                        NeptuneEngineVersionUtil.NeptuneDBParameterGroupFamily.NEPTUNE1_2,
                        NeptuneEngineVersionUtil.NeptuneClusterParameterGroupFamily.NEPTUNE1_2},
                new Object[]{"1.0.5.0.R5",
                        NeptuneEngineVersionUtil.NeptuneEngineVersion.builder()
                                .majorVersion("1.0")
                                .minorVersion("5.0")
                                .build(),
                        NeptuneEngineVersionUtil.NeptuneDBParameterGroupFamily.NEPTUNE1,
                        NeptuneEngineVersionUtil.NeptuneClusterParameterGroupFamily.NEPTUNE1},
                new Object[]{"1.1.1.0.R2",
                        NeptuneEngineVersionUtil.NeptuneEngineVersion.builder()
                                .majorVersion("1.1")
                                .minorVersion("1.0")
                                .build(),
                        NeptuneEngineVersionUtil.NeptuneDBParameterGroupFamily.NEPTUNE1,
                        NeptuneEngineVersionUtil.NeptuneClusterParameterGroupFamily.NEPTUNE1},
                new Object[]{"1.2.0.0.R1",
                        NeptuneEngineVersionUtil.NeptuneEngineVersion.builder()
                                .majorVersion("1.2")
                                .minorVersion("0.0")
                                .build(),
                        NeptuneEngineVersionUtil.NeptuneDBParameterGroupFamily.NEPTUNE1_2,
                        NeptuneEngineVersionUtil.NeptuneClusterParameterGroupFamily.NEPTUNE1_2},
                new Object[]{"1.2.0.1.R10",
                        NeptuneEngineVersionUtil.NeptuneEngineVersion.builder()
                                .majorVersion("1.2")
                                .minorVersion("0.1")
                                .build(),
                        NeptuneEngineVersionUtil.NeptuneDBParameterGroupFamily.NEPTUNE1_2,
                        NeptuneEngineVersionUtil.NeptuneClusterParameterGroupFamily.NEPTUNE1_2},
        };
    }

    @Test
    public void testParseNeptuneEngineVersion() {
        final NeptuneEngineVersionUtil.NeptuneEngineVersion parsedEngineVersion =
                NeptuneEngineVersionUtil.parseNeptuneEngineVersion(this.engineVersion);
        Assert.assertEquals(parsedEngineVersion.getMajorVersion(), expectedNeptuneEngineVersion.getMajorVersion());
        Assert.assertEquals(parsedEngineVersion.getMinorVersion(), expectedNeptuneEngineVersion.getMinorVersion());
    }

    @Test
    public void testGetNeptuneClusterParameterGroupFamily() {
        final NeptuneEngineVersionUtil.NeptuneClusterParameterGroupFamily actualNeptuneClusterParameterGroupFamily =
                NeptuneEngineVersionUtil.getNeptuneClusterParameterGroupFamily(this.engineVersion);
        Assert.assertEquals(actualNeptuneClusterParameterGroupFamily, expectedNeptuneClusterParameterGroupFamily);
    }

    @Test
    public void testGetNeptuneDBParameterGroupFamily() {
        final NeptuneEngineVersionUtil.NeptuneDBParameterGroupFamily actualNeptuneDBParameterGroupFamily =
                NeptuneEngineVersionUtil.getNeptuneDBParameterGroupFamily(this.engineVersion);
        Assert.assertEquals(actualNeptuneDBParameterGroupFamily, expectedNeptuneDBParameterGroupFamily);
    }
}
