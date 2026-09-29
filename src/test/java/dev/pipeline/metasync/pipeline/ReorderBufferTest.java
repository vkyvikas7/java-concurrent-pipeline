package dev.pipeline.metasync.pipeline;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReorderBufferTest {

    @Test
    void emitsItemsOnlyWhenTheNextSequenceIsPresent() {
        ReorderBuffer<String> buffer = new ReorderBuffer<>();

        assertThat(buffer.push(2, "c")).isEmpty();
        assertThat(buffer.push(1, "b")).isEmpty();
        assertThat(buffer.push(0, "a")).containsExactly("a", "b", "c");
        assertThat(buffer.pending()).isZero();
        assertThat(buffer.highWatermark()).isEqualTo(3);
    }

    @Test
    void rejectsDuplicateAndRegressiveSequences() {
        ReorderBuffer<String> buffer = new ReorderBuffer<>();
        buffer.push(0, "a");

        assertThatThrownBy(() -> buffer.push(0, "again"))
                .isInstanceOf(PipelineException.class)
                .hasMessageContaining("behind");
        buffer.push(2, "c");
        assertThatThrownBy(() -> buffer.push(2, "c2"))
                .isInstanceOf(PipelineException.class)
                .hasMessageContaining("duplicate");
    }
}
