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

import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.templating.VariableTemplatePart.Parameter;

public final class FormatterUtils {

    public static final BiFunction<SinkRecord, Parameter, String> formatKafkaOffset =
        (sinkRecord, usePaddingParameter) ->
            usePaddingParameter.asBoolean()
                ? String.format("%020d", sinkRecord.kafkaOffset())
                : Long.toString(sinkRecord.kafkaOffset());

    public static String formatKafkaOffset(final SinkRecord record) {
        return formatKafkaOffset.apply(record, Parameter.of("padding", "true"));
    }

    public static final BiFunction<TimestampSource, Parameter, String> formatTimestamp =
        new BiFunction<>() {

            private final Map<String, DateTimeFormatter> fomatterMap =
                Map.of(
                    "YYYY", DateTimeFormatter.ofPattern("YYYY"),
                    "MM", DateTimeFormatter.ofPattern("MM"),
                    "dd", DateTimeFormatter.ofPattern("dd"),
                    "HH", DateTimeFormatter.ofPattern("HH")
                );

            @Override
            public String apply(final TimestampSource tsSource, final Parameter parameter) {
                return tsSource.time().format(fomatterMap.get(parameter.value()));
            }

        };

}
