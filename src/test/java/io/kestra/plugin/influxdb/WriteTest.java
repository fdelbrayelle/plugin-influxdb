package io.kestra.plugin.influxdb;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class WriteTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        var testData = """
            cpu,host=server01,region=us_west value=0.64 1422568543702900257
            mem,host=server01,region=us_west free=1024,total=4096 1422568543702900260
            """;

        var runContext = runContextFactory.of(Map.of());

        var task = Write.builder()
            .url("http://localhost:8086")
            .token("my-token")
            .org("my-org")
            .bucket("my-bucket")
            .wireInputMultilineData(new Property<>(testData))
            .build();

        var runOutput = task.run(runContext);

        assertThat(runOutput, is(notNullValue()));
        assertThat(runOutput.getCount(), is(notNullValue()));
        assertThat(runOutput.getCount(), is(2));
    }
}
