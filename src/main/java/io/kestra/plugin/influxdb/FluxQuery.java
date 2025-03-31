package io.kestra.plugin.influxdb;

import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.query.FluxRecord;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query using Flux",
    description = "Query measurements using Flux"
)
@Plugin(
    examples = {
        @io.kestra.core.models.annotations.Example(
            title = "Query measurements using Flux",
            code = """
                    id: write
                    namespace: io.kestra.influxdb
                    tasks:
                      - id: query-metrics
                        type: io.kestra.plugin.influxdb.FluxQuery
                        url: http://localhost:8086
                        token: my-token
                        org: my-org
                        bucket: my-bucket
                        query: |
                          from(bucket: "my-bucket")
                            |> range(start: 2015-01-29T21:55:00Z, stop: 2015-01-29T21:56:00Z)
                            |> filter(fn: (r) => r["_measurement"] == "cpu")
                            |> mean()
                """
        )
    }
)
public class FluxQuery extends Task implements RunnableTask<FluxQuery.Output> {

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

    @Schema(
        title = "Flux query",
        description = "The Flux query to execute"
    )
    private Property<String> query;

    @Schema(
        title = "The way you want to store the data.",
        description = "FETCH_ONE output the first row, "
            + "FETCH output all the rows, "
            + "STORE store all rows in a file, "
            + "NONE do nothing."
    )
    @Builder.Default
    private Property<FetchType> fetchType = Property.of(FetchType.FETCH);

    @Override
    public FluxQuery.Output run(RunContext runContext) throws Exception {
        var query = runContext.render(this.query).as(String.class).orElseThrow();
        var fetchTypeValue = runContext.render(fetchType).as(FetchType.class).orElseThrow();

        try (var influxDBClient = InfluxDBClientFactory.create(
            url,
            token.toCharArray(),
            org,
            bucket
        )) {
            var queryApi = influxDBClient.getQueryApi();
            var tables = queryApi.query(query);

            if (tables.isEmpty()) {
                return Output.builder()
                    .count(0)
                    .build();
            }

            var rows = tables.stream()
                .flatMap(table -> table.getRecords().stream())
                .map(this::recordToMap)
                .toList();

            if (rows.isEmpty()) {
                return Output.builder()
                    .count(0)
                    .build();
            }

            var uri = "";
            if (fetchTypeValue == FetchType.STORE) {
                var tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                try (var output = new FileOutputStream(tempFile)) {
                    FileSerde.write(output, rows);
                    uri = runContext.storage().putFile(tempFile).toString();
                }
            }

            return Output.builder()
                .count(rows.size())
                .row(fetchTypeValue == FetchType.FETCH_ONE ? rows.isEmpty() ? null : rows.getFirst() : null)
                .rows(fetchTypeValue == FetchType.FETCH ? rows : null)
                .uri(fetchTypeValue == FetchType.STORE ? uri : null)
                .build();
        }
    }

    private Map<String, Object> recordToMap(FluxRecord record) {
        return record.getValues().entrySet().stream()
            .filter(entry -> entry.getValue() != null)
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                    Object value = entry.getValue();
                    if (value instanceof Number) {
                        if (value instanceof Float || value instanceof Double) {
                            return ((Number) value).doubleValue();
                        } else {
                            return ((Number) value).longValue();
                        }
                    }
                    return value;
                }
            ));
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Measurements count",
            description = "Number of rows returned by the query"
        )
        private final int count;

        @Schema(
            title = "First row",
            description = "First row of the query result when using FETCH_ONE"
        )
        private final Map<String, Object> row;

        @Schema(
            title = "All rows",
            description = "All rows from the query result when using FETCH"
        )
        private final List<Map<String, Object>> rows;

        @Schema(
            title = "URI to the stored file",
            description = "URI to the file containing the results when using STORE"
        )
        private final String uri;
    }
}
