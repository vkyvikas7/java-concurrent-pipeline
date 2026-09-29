package dev.pipeline.metasync.api;

import dev.pipeline.metasync.pipeline.PipelineException;
import dev.pipeline.metasync.service.RejectedRunException;
import dev.pipeline.metasync.service.SyncInProgressException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.List;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiError> validation(MethodArgumentNotValidException exception) {
        List<String> details = exception.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage())
                .toList();
        return ResponseEntity.badRequest().body(new ApiError(
                400,
                "Bad Request",
                "request failed validation",
                details
        ));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiError> unreadable(HttpMessageNotReadableException exception) {
        return ResponseEntity.badRequest().body(new ApiError(
                400,
                "Bad Request",
                "request body could not be read",
                List.of()
        ));
    }

    @ExceptionHandler({RejectedRunException.class, IllegalArgumentException.class})
    public ResponseEntity<ApiError> rejected(RuntimeException exception) {
        return ResponseEntity.badRequest().body(new ApiError(
                400,
                "Bad Request",
                exception.getMessage(),
                List.of()
        ));
    }

    @ExceptionHandler(SyncInProgressException.class)
    public ResponseEntity<ApiError> busy(SyncInProgressException exception) {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(new ApiError(
                429,
                "Too Many Requests",
                exception.getMessage(),
                List.of()
        ));
    }

    @ExceptionHandler(PipelineException.class)
    public ResponseEntity<ApiError> pipeline(PipelineException exception) {
        return ResponseEntity.internalServerError().body(new ApiError(
                500,
                "Pipeline Failed",
                exception.getMessage(),
                List.of()
        ));
    }
}
