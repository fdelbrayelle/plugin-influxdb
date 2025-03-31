package io.kestra.plugin.influxdb;

import io.kestra.core.junit.annotations.ExecuteFlow;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * This test will execute the query.yaml flow that queries metrics from InfluxDB
 * and verifies the output format based on the fetchType.
 */
@KestraTest(startRunner = true)
class FluxQueryRunnerTest {

    @Test
    @ExecuteFlow("flows/query.yaml")
    void flow(Execution execution) {
        // Verify we have both tasks executed
        assertThat(execution.getTaskRunList(), hasSize(2));

        // Get the output from the query-metrics task
        var tasksRuns = execution.findTaskRunsByTaskId("query-metrics");
        assertThat(tasksRuns, hasSize(1));

        var queryMetricsOutput = tasksRuns
            .getFirst()
            .getOutputs();

        // Verify we got results
        assertThat(queryMetricsOutput.get("count"), is(notNullValue()));
    }
}
