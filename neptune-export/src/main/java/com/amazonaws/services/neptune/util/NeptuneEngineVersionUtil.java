package com.amazonaws.services.neptune.util;

import com.google.common.base.Preconditions;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

public class NeptuneEngineVersionUtil {
    @Value
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Builder
    public static class NeptuneEngineVersion {
        @NonNull String majorVersion;
        @NonNull String minorVersion;

        @Override
        public String toString() {
            return majorVersion + "." + minorVersion;
        }
    }

    @RequiredArgsConstructor
    @Getter
    public enum NeptuneClusterParameterGroupFamily {
        NEPTUNE1("neptune1"),
        NEPTUNE1_2("neptune1.2");

        private final String parameterGroupFamily;
    }

    @RequiredArgsConstructor
    @Getter
    public enum NeptuneDBParameterGroupFamily {
        NEPTUNE1("neptune1"),
        NEPTUNE1_2("neptune1.2");

        private final String parameterGroupFamily;
    }

    public static NeptuneEngineVersion parseNeptuneEngineVersion(@NonNull final String engineVersion) {
        Preconditions.checkArgument(StringUtils.isNotBlank(engineVersion), "Engine version cannot be blank");
        final String[] tokens = engineVersion.split("\\.");
        Preconditions.checkArgument(tokens.length >= 4, "Invalid engine version " + engineVersion);

        // For engine version 1.2.0.1.R1, majorVersion will be 1.2 and minor version is 0.1
        final String majorVersion = String.join(".", ArrayUtils.subarray(tokens, 0, 2));
        final String minorVersion = String.join(".", ArrayUtils.subarray(tokens, 2, 4));
        return NeptuneEngineVersion.builder().majorVersion(majorVersion).minorVersion(minorVersion).build();
    }

    public static NeptuneClusterParameterGroupFamily getNeptuneClusterParameterGroupFamily(@NonNull final String engineVersion) {
        return getNeptuneClusterParameterGroupFamily(parseNeptuneEngineVersion(engineVersion));
    }

    public static NeptuneClusterParameterGroupFamily getNeptuneClusterParameterGroupFamily(@NonNull final NeptuneEngineVersion engineVersion) {
        final String majorVersion = engineVersion.getMajorVersion();
        switch (majorVersion) {
            case "1.0":
            case "1.1":
                return NeptuneClusterParameterGroupFamily.NEPTUNE1;
            case "1.2":
                return NeptuneClusterParameterGroupFamily.NEPTUNE1_2;
            default:
                throw new IllegalArgumentException("Unsupported major engine version: " + engineVersion);
        }
    }

    public static NeptuneDBParameterGroupFamily getNeptuneDBParameterGroupFamily(@NonNull final String engineVersion) {
        return getNeptuneDBParameterGroupFamily(parseNeptuneEngineVersion(engineVersion));
    }

    public static NeptuneDBParameterGroupFamily getNeptuneDBParameterGroupFamily(@NonNull final NeptuneEngineVersion engineVersion) {
        final String majorVersion = engineVersion.getMajorVersion();
        switch (majorVersion) {
            case "1.0":
            case "1.1":
                return NeptuneDBParameterGroupFamily.NEPTUNE1;
            case "1.2":
                return NeptuneDBParameterGroupFamily.NEPTUNE1_2;
            default:
                throw new IllegalArgumentException("Unsupported major engine version: " + engineVersion);
        }
    }

}
