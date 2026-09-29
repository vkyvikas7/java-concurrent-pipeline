package dev.pipeline.metasync.api;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class SyncControllerTest {

    @Autowired
    private MockMvc mvc;

    @Test
    void describesTheDemoEndpoint() throws Exception {
        mvc.perform(get("/api/v1/sync"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.service").value("metadata-sync"))
                .andExpect(jsonPath("$.example.mode").value("BOTH"));
    }

    @Test
    void runsBothEnginesAndReturnsCounts() throws Exception {
        mvc.perform(post("/api/v1/sync/run")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "mode": "BOTH",
                                  "itemCount": 40,
                                  "latencyMillis": 0,
                                  "workerCount": 4,
                                  "queueCapacity": 8,
                                  "batchSize": 10,
                                  "ordering": "ORDERED"
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.sequential.succeeded").value(40))
                .andExpect(jsonPath("$.sequential.failed").value(0))
                .andExpect(jsonPath("$.concurrent.succeeded").value(40))
                .andExpect(jsonPath("$.concurrent.failed").value(0))
                .andExpect(jsonPath("$.concurrent.workerCount").value(4))
                .andExpect(jsonPath("$.concurrent.sampleAssets[0].eventId").value("evt-0"))
                .andExpect(jsonPath("$.concurrent.sampleAssets[0].classification").value("STRUCTURED"))
                .andExpect(jsonPath("$.summary").exists());
    }

    @Test
    void rejectsAnInvalidBody() throws Exception {
        mvc.perform(post("/api/v1/sync/run")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"mode":"CONCURRENT","itemCount":0,"latencyMillis":10}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Bad Request"))
                .andExpect(jsonPath("$.details[0]").exists());
    }

    @Test
    void appliesDefaultsWhenOptionalFieldsAreOmitted() throws Exception {
        mvc.perform(post("/api/v1/sync/run")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"mode":"CONCURRENT","itemCount":12,"latencyMillis":0}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.concurrent.succeeded").value(12))
                .andExpect(jsonPath("$.concurrent.workerCount").value(8))
                .andExpect(jsonPath("$.concurrent.queueCapacity").value(32))
                .andExpect(jsonPath("$.concurrent.batchSize").value(64))
                .andExpect(jsonPath("$.sequential").doesNotExist());
    }
}
