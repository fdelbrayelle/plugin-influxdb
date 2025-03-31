package io.kestra.plugin.influxdb;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.domain.WritePrecision;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Write a measure",
    description = "Write a measure from a wire format multiline string"
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Write a measure from a WIRE format multiline string",
            code = """
                    id: write
                    namespace: io.kestra.influxdb
                    tasks:
                      - id: write-metrics
                        type: io.kestra.plugin.influxdb.Write
                        url: http://localhost:8086
                        token: my-token
                        org: my-org
                        bucket: my-bucket
                        wireInputMultilineData: |
                          cpu,host=server01,region=us_west value=0.64 1422568543702900257
                          mem,host=server01,region=us_west free=1024,total=4096 1422568543702900260
                      - id: verify-output
                        type: io.kestra.core.tasks.debugs.Return
                        format: "{{outputs['write-metrics'].count}}"

                """
        )
    }
)
public class Write extends Task implements RunnableTask<Write.Output> {

    @Schema(
        title = "InfluxDB URL",
        description = "The URL of the InfluxDB server"
    )
    @Builder.Default
    private String url = "http://localhost:8086";

    @Schema(
        title = "InfluxDB token",
        description = "The authentication token for InfluxDB"
    )
    @Builder.Default
    private String token = "my-token";

    @Schema(
        title = "InfluxDB organization",
        description = "The organization name in InfluxDB"
    )
    @Builder.Default
    private String org = "my-org";

    @Schema(
        title = "InfluxDB bucket",
        description = "The bucket name in InfluxDB"
    )
    @Builder.Default
    private String bucket = "my-bucket";

    /**
     * Examples:
     * cpu,host=server01,region=us_west value=0.64 1422568543702900257
     * cpu,host=server01,region=us_west value=0.66 1422568543702900258
     * cpu,host=server02,region=eu_central value=0.88 1422568543702900259
     * mem,host=server01,region=us_west free=1024,total=4096 1422568543702900260
     * mem,host=server02,region=eu_central free=2048,total=8192 1422568543702900261
     * http_requests,method=GET,status=200 count=1 1422568543702900262
     * http_requests,method=POST,status=400 count=3 1422568543702900263
     */
    @Schema(
        title = "Wire format data",
        description = "The data to write in InfluxDB Line Protocol format"
    )
    private Property<String> wireInputMultilineData;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rawWireInputMultilineData = runContext.render(wireInputMultilineData).as(String.class).orElse("");

        try (InfluxDBClient influxDBClient = InfluxDBClientFactory.create(
            url,
            token.toCharArray(),
            org,
            bucket
        )) {
            var writeApi = influxDBClient.getWriteApiBlocking();
            writeApi.writeRecord(bucket, org, WritePrecision.NS, rawWireInputMultilineData);

            // Count the number of lines written
            var count = rawWireInputMultilineData.lines()
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .count();

            return Output.builder()
                .count((int) count)
                .build();
        }
    }

    public record Measurement(
        String name,
        Map<String, String> tags,
        Map<String, Object> fields,
        Long timestamp
    ) {
        public static Measurement fromWireLine(String line) {
            var parts = line.trim().split("\\s+");
            if (parts.length != 3) {
                throw new IllegalArgumentException("Invalid WIRE format line: " + line);
            }

            var measurementAndTags = parts[0].split(",", 2);
            var name = measurementAndTags[0];
            Map<String, String> tags = new HashMap<>();
            if (measurementAndTags.length > 1) {
                String[] tagPairs = measurementAndTags[1].split(",");
                for (String pair : tagPairs) {
                    String[] keyValue = pair.split("=", 2);
                    tags.put(keyValue[0], keyValue[1]);
                }
            }

            Map<String, Object> fields = new HashMap<>();
            var fieldPairs = parts[1].split(",");
            for (var pair : fieldPairs) {
                var keyValue = pair.split("=", 2);
                var value = keyValue[1];
                try {
                    if (value.contains(".")) {
                        fields.put(keyValue[0], Double.parseDouble(value));
                    } else {
                        fields.put(keyValue[0], Long.parseLong(value));
                    }
                } catch (NumberFormatException e) {
                    fields.put(keyValue[0], value.replaceAll("^\"|\"$", ""));
                }
            }

            var timestamp = Long.parseLong(parts[2]);

            return new Measurement(name, tags, fields, timestamp);
        }

        // Convert multiple WIRE format lines into a list of Measurements
        public static List<Measurement> fromWireLines(String multilineData) {
            return Arrays.stream(multilineData.split("\n"))
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .map(Measurement::fromWireLine)
                .toList();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Measurements count",
            description = "How many measurements have been written to InfluxDB"
        )
        private final int count;
    }
}
