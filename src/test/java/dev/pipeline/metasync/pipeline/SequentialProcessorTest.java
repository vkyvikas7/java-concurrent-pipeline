package dev.pipeline.metasync.pipeline;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SequentialProcessorTest {

    private final SequentialProcessor<Integer, Integer> processor = new SequentialProcessor<>();

    @Test
    void preservesOrderAndFlushesFullPlusRemainderBatches() {
        List<Integer> inputs = range(100);
        List<List<Integer>> batches = new ArrayList<>();
        PipelineResult result = processor.process(inputs, item -> item, batches::add, 32);

        assertThat(result.succeeded()).isEqualTo(100);
        assertThat(result.failed()).isZero();
        assertThat(result.batchesFlushed()).isEqualTo(4);
        assertThat(batches).hasSize(4);
        assertThat(batches.get(0)).hasSize(32);
        assertThat(batches.get(3)).hasSize(4);
        assertThat(flatten(batches)).containsExactlyElementsOf(inputs);
        assertThat(result.engine()).isEqualTo(PipelineResult.Engine.SEQUENTIAL);
        assertThat(result.workerCount()).isEqualTo(1);
    }

    @Test
    void isolatesItemFailuresAndStillFlushesSuccesses() {
        List<Integer> inputs = range(100);
        List<Integer> written = new ArrayList<>();
        PipelineResult result = processor.process(inputs, item -> {
            if (item % 10 == 0) {
                throw new IllegalStateException("bad-" + item);
            }
            return item;
        }, written::addAll, 32);

        assertThat(result.succeeded()).isEqualTo(90);
        assertThat(result.failed()).isEqualTo(10);
        assertThat(result.batchesFlushed()).isEqualTo(3);
        assertThat(written).hasSize(90);
        assertThat(written).doesNotContain(0, 10, 90);
        assertThat(result.sampleErrors()).anyMatch(error -> error.contains("bad-0"));
        assertThat(result.sampleErrors()).hasSize(10);
    }

    @Test
    void emptyInputDoesNotFlush() {
        AtomicInteger flushes = new AtomicInteger();
        PipelineResult result = processor.process(List.of(), item -> item, batch -> flushes.incrementAndGet(), 8);

        assertThat(result.submitted()).isZero();
        assertThat(result.batchesFlushed()).isZero();
        assertThat(flushes).hasValue(0);
    }

    @Test
    void rejectsNullItemsAndInvalidBatchSize() {
        assertThatThrownBy(() -> processor.process(java.util.Arrays.asList(1, null), item -> item, batch -> {
        }, 8)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> processor.process(List.of(1), item -> item, batch -> {
        }, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    private static List<Integer> range(int count) {
        List<Integer> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            values.add(i);
        }
        return values;
    }

    private static List<Integer> flatten(List<List<Integer>> batches) {
        List<Integer> all = new ArrayList<>();
        batches.forEach(all::addAll);
        return all;
    }
}
