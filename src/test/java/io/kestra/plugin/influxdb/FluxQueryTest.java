package io.kestra.plugin.influxdb;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@TestInstance(Lifecycle.PER_CLASS)
class FluxQueryTest {
    @Inject
    private RunContextFactory runContextFactory;

    @BeforeAll
    void beforeAll() throws Exception {
        var testData = """
                cpu,host=server01,region=us_west value=0.64 1422568543702900257
                cpu,host=server01,region=us_west value=0.66 1422568543702900258
                cpu,host=server02,region=eu_central value=0.88 1422568543702900259
                mem,host=server01,region=us_west free=1024,total=4096 1422568543702900260
                mem,host=server02,region=eu_central free=2048,total=8192 1422568543702900261
            """;

        var runContext = runContextFactory.of(Map.of());

        var task = Write.builder()
            .url("http://localhost:8086")
            .token("my-token")
            .org("my-org")
            .bucket("my-bucket")
            .wireInputMultilineData(new Property<>(testData))
            .build();

        task.run(runContext);
    }

    @Test
    void runFetchOne() throws Exception {
        var query = """
            from(bucket: "my-bucket")
              |> range(start: 2015-01-29T21:55:43Z, stop: 2015-01-29T21:55:44Z)
              |> filter(fn: (r) => r["_measurement"] == "cpu")
              |> mean()
            """;

        var runContext = runContextFactory.of(Map.of());

        var task = FluxQuery.builder()
            .url("http://localhost:8086")
            .token("my-token")
            .org("my-org")
            .bucket("my-bucket")
            .query(new Property<>(query))
            .fetchType(Property.of(FetchType.FETCH_ONE))
            .build();

        var runOutput = task.run(runContext);

        assertThat(runOutput, is(notNullValue()));
        assertThat(runOutput.getCount(), is(notNullValue()));
        assertThat(runOutput.getRow(), is(notNullValue()));
        assertThat(runOutput.getRows(), is(nullValue()));
        assertThat(runOutput.getUri(), is(nullValue()));
    }

    @Test
    void runFetch() throws Exception {
        var query = """
            from(bucket: "my-bucket")
              |> range(start: 2015-01-29T21:55:43Z, stop: 2015-01-29T21:55:44Z)
              |> filter(fn: (r) => r["_measurement"] == "cpu")
              |> mean()
            """;

        var runContext = runContextFactory.of(Map.of());

        var task = FluxQuery.builder()
            .url("http://localhost:8086")
            .token("my-token")
            .org("my-org")
            .bucket("my-bucket")
            .query(new Property<>(query))
            .fetchType(Property.of(FetchType.FETCH))
            .build();

        var runOutput = task.run(runContext);

        assertThat(runOutput, is(notNullValue()));
        assertThat(runOutput.getCount(), is(notNullValue()));
        assertThat(runOutput.getRow(), is(nullValue()));
        assertThat(runOutput.getRows(), is(notNullValue()));
        assertThat(runOutput.getUri(), is(nullValue()));
    }

    @Test
    void runStore() throws Exception {
        var query = """
            from(bucket: "my-bucket")
              |> range(start: 2015-01-29T21:55:43Z, stop: 2015-01-29T21:55:44Z)
              |> filter(fn: (r) => r["_measurement"] == "cpu")
              |> mean()
            """;

        var runContext = runContextFactory.of(Map.of());

        var task = FluxQuery.builder()
            .url("http://localhost:8086")
            .token("my-token")
            .org("my-org")
            .bucket("my-bucket")
            .query(new Property<>(query))
            .fetchType(Property.of(FetchType.STORE))
            .build();

        var runOutput = task.run(runContext);

        assertThat(runOutput, is(notNullValue()));
        assertThat(runOutput.getCount(), is(notNullValue()));
        assertThat(runOutput.getRow(), is(nullValue()));
        assertThat(runOutput.getRows(), is(nullValue()));
        assertThat(runOutput.getUri(), is(notNullValue()));
    }
}
