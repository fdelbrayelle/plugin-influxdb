package io.kestra.plugin.influxdb;

import io.kestra.core.junit.annotations.ExecuteFlow;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * This test will execute the write.yaml flow that writes metrics to InfluxDB
 * and verifies the count of measurements written.
 */
@KestraTest(startRunner = true)
class WriteRunnerTest {
    @Test
    @ExecuteFlow("flows/write.yaml")
    void flow(Execution execution) throws TimeoutException, QueueException {
        // Verify we have both tasks executed
        assertThat(execution.getTaskRunList(), hasSize(2));

        // Get the output from the verify-output task which contains the count
        var tasksRuns = execution.findTaskRunsByTaskId("write-metrics");
        assertThat(tasksRuns, hasSize(1));

        var writeMetricsOutput = tasksRuns
            .getFirst()
            .getOutputs();

        // Verify we wrote 2 measurements
        assertThat(writeMetricsOutput.get("count"), is(2));
    }
}
