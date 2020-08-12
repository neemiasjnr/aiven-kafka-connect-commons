/*
 * Copyright 2020 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.common.config;

import io.aiven.kafka.connect.common.config.validators.TimeZoneValidator;
import io.aiven.kafka.connect.common.config.validators.TimestampSourceValidator;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.connect.common.config.validators.FileCompressionTypeValidator;
import io.aiven.kafka.connect.common.config.validators.OutputFieldsEncodingValidator;
import io.aiven.kafka.connect.common.config.validators.OutputFieldsValidator;
import io.aiven.kafka.connect.common.templating.Template;
import org.apache.kafka.common.config.ConfigException;

public class AivenCommonConfig extends AbstractConfig {
    public static final String FORMAT_OUTPUT_FIELDS_CONFIG = "format.output.fields";
    public static final String FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG = "format.output.fields.value.encoding";
    public static final String FILE_COMPRESSION_TYPE_CONFIG = "file.compression.type";
    public static final String FILE_MAX_RECORDS = "file.max.records";
    public static final String FILE_NAME_TIMESTAMP_TIMEZONE = "file.name.timestamp.timezone";
    public static final String FILE_NAME_TIMESTAMP_SOURCE = "file.name.timestamp.source";
    public static final String FILE_NAME_TEMPLATE_CONFIG = "file.name.template";
    public static final String FILE_NAME_PREFIX_CONFIG = "file.name.prefix";
    public static final String NAME_CONFIG = "name";
    public static final String GROUP_AWS = "AWS";
    public static final String GROUP_FILE = "File";
    public static final String GROUP_FORMAT = "Format";
    public static final String DEFAULT_FILENAME_TEMPLATE = "{{topic}}-{{partition}}-{{start_offset}}";

    protected AivenCommonConfig(final ConfigDef definition, final Map<?, ?> originals) {
        super(definition, originals);
    }

    protected static void addFileGroupConfig(final ConfigDef configDef) {
        int fileGroupCounter = 0;

        configDef.define(
            FILE_NAME_TEMPLATE_CONFIG,
            ConfigDef.Type.STRING,
            null,
            new FilenameTemplateValidator(FILE_NAME_TEMPLATE_CONFIG),
            ConfigDef.Importance.MEDIUM,
            "The template for file names on GCS. "
                + "Supports `{{ variable }}` placeholders for substituting variables. "
                + "Currently supported variables are `topic`, `partition`, and `start_offset` "
                + "(the offset of the first record in the file). "
                + "Only some combinations of variables are valid, which currently are:\n"
                + "- `topic`, `partition`, `start_offset`.",
            GROUP_FILE,
            fileGroupCounter++,
            ConfigDef.Width.LONG,
            FILE_NAME_TEMPLATE_CONFIG
        );

        configDef.define(
            FILE_COMPRESSION_TYPE_CONFIG,
            ConfigDef.Type.STRING,
            null,
            new FileCompressionTypeValidator(),
            ConfigDef.Importance.MEDIUM,
            "The compression type used for files put on AWS. "
                + "The supported values are: " + CompressionType.SUPPORTED_COMPRESSION_TYPES + ".",
            GROUP_FILE,
            fileGroupCounter++,
            ConfigDef.Width.NONE,
            FILE_COMPRESSION_TYPE_CONFIG,
            FixedSetRecommender.ofSupportedValues(CompressionType.names())
        );

        configDef.define(
            FILE_MAX_RECORDS,
            ConfigDef.Type.INT,
            0,
            new ConfigDef.Validator() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    assert value instanceof Integer;
                    if ((Integer) value < 0) {
                        throw new ConfigException(
                            FILE_MAX_RECORDS, value,
                            "must be a non-negative integer number");
                    }
                }
            },
            ConfigDef.Importance.MEDIUM,
            "The maximum number of records to put in a single file. "
                + "Must be a non-negative integer number. "
                + "0 is interpreted as \"unlimited\", which is the default.",
            GROUP_FILE,
            fileGroupCounter,
            ConfigDef.Width.SHORT,
            FILE_MAX_RECORDS
        );

        configDef.define(
            FILE_NAME_TIMESTAMP_TIMEZONE,
            ConfigDef.Type.STRING,
            ZoneOffset.UTC.toString(),
            new TimeZoneValidator(),
            ConfigDef.Importance.LOW,
            "Specifies the timezone in which the dates and time for the timestamp variable will be treated. "
                + "Use standard shot and long names. Default is UTC",
            GROUP_FILE,
            fileGroupCounter++,
            ConfigDef.Width.SHORT,
            FILE_NAME_TIMESTAMP_TIMEZONE
        );

        configDef.define(
            FILE_NAME_TIMESTAMP_SOURCE,
            ConfigDef.Type.STRING,
            TimestampSource.Type.WALLCLOCK.name(),
            new TimestampSourceValidator(),
            ConfigDef.Importance.LOW,
            "Specifies the the timestamp variable source. Default is wall-clock.",
            GROUP_FILE,
            fileGroupCounter,
            ConfigDef.Width.SHORT,
            FILE_NAME_TIMESTAMP_SOURCE
        );
    }

    protected static void addFormatGroupConfig(final ConfigDef configDef) {
        int formatGroupCounter = 0;

        configDef.define(
            FORMAT_OUTPUT_FIELDS_CONFIG,
            ConfigDef.Type.LIST,
            null,
            new OutputFieldsValidator(),
            ConfigDef.Importance.MEDIUM,
            "Fields to put into output files. "
                + "The supported values are: " + OutputField.SUPPORTED_OUTPUT_FIELDS + ".",
            GROUP_FORMAT,
            formatGroupCounter++,
            ConfigDef.Width.NONE,
            FORMAT_OUTPUT_FIELDS_CONFIG,
            FixedSetRecommender.ofSupportedValues(OutputFieldType.names())
        );

        configDef.define(
            FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG,
            ConfigDef.Type.STRING,
            OutputFieldEncodingType.BASE64.name,
            new OutputFieldsEncodingValidator(),
            ConfigDef.Importance.MEDIUM,
            "The type of encoding for the value field. "
                + "The supported values are: " + OutputFieldEncodingType.SUPPORTED_FIELD_ENCODING_TYPES + ".",
            GROUP_FORMAT,
            formatGroupCounter,
            ConfigDef.Width.NONE,
            FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG,
            FixedSetRecommender.ofSupportedValues(OutputFieldEncodingType.names())
        );
    }

    public final String getConnectorName() {
        return originalsStrings().get(NAME_CONFIG);
    }

    public CompressionType getCompressionType() {
        return CompressionType.forName(getString(FILE_COMPRESSION_TYPE_CONFIG));
    }

    public OutputFieldEncodingType getOutputFieldEncodingType() {
        return OutputFieldEncodingType.forName(getString(FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG));
    }

    public final Template getFilenameTemplate() {
        return Template.of(getFilename());
    }

    public final String getFilename() {
        return resolveFilenameTemplate();
    }

    private String resolveFilenameTemplate() {
        String fileNameTemplate = getString(FILE_NAME_TEMPLATE_CONFIG);
        if (fileNameTemplate == null) {
            fileNameTemplate = DEFAULT_FILENAME_TEMPLATE + getCompressionType().extension();
        }
        return fileNameTemplate;
    }

    public final ZoneId getFilenameTimezone() {
        return ZoneId.of(getString(FILE_NAME_TIMESTAMP_TIMEZONE));
    }

    public final TimestampSource getFilenameTimestampSource() {
        return TimestampSource.of(
            getFilenameTimezone(),
            TimestampSource.Type.of(getString(FILE_NAME_TIMESTAMP_SOURCE))
        );
    }

    public final int getMaxRecordsPerFile() {
        return getInt(FILE_MAX_RECORDS);
    }

    public List<OutputField> getOutputFields() {
        final List<OutputField> result = new ArrayList<>();
        for (final String outputFieldTypeStr : getList(FORMAT_OUTPUT_FIELDS_CONFIG)) {
            final OutputFieldType fieldType = OutputFieldType.forName(outputFieldTypeStr);
            final OutputFieldEncodingType encodingType;
            if (fieldType == OutputFieldType.VALUE || fieldType == OutputFieldType.KEY) {
                encodingType = getOutputFieldEncodingType();
            } else {
                encodingType = OutputFieldEncodingType.NONE;
            }
            result.add(new OutputField(fieldType, encodingType));
        }
        return result;
    }
}
