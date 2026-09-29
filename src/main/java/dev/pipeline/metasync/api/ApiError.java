package dev.pipeline.metasync.api;

import java.util.List;

public record ApiError(
        int status,
        String error,
        String message,
        List<String> details
) {
}
