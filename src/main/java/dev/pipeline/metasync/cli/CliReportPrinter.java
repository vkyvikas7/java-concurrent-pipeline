package dev.pipeline.metasync.cli;

import dev.pipeline.metasync.service.SyncReport;
import dev.pipeline.metasync.service.SyncRunResponse;

import java.io.PrintStream;
import java.util.Locale;

/**
 * Plain-text table for the CLI demo. The HTTP API returns the same numbers as JSON.
 */
public final class CliReportPrinter {

    private CliReportPrinter() {
    }

    public static void print(PrintStream out, SyncRunResponse response) {
        out.println("metadata sync benchmark");
        out.println("--------------------------------------------------------------------------------------------------");
        out.printf(Locale.US, "%-12s %8s %8s %8s %8s %10s %12s %14s %10s%n",
                "engine", "workers", "items", "ok", "fail", "wall_ms", "items/sec", "blocked_in", "ingress_hw");
        if (response.sequential() != null) {
            printRow(out, response.sequential());
        }
        if (response.concurrent() != null) {
            printRow(out, response.concurrent());
        }
        out.println("--------------------------------------------------------------------------------------------------");
        out.println(response.summary());
        if (response.speedup() != null) {
            out.printf(Locale.US, "speedup (sequential_wall / concurrent_wall): %.2fx%n", response.speedup());
        }
    }

    private static void printRow(PrintStream out, SyncReport report) {
        out.printf(Locale.US, "%-12s %8d %8d %8d %8d %10d %12.2f %14d %10d%n",
                report.engine(),
                report.workerCount(),
                report.submitted(),
                report.succeeded(),
                report.failed(),
                report.wallClockMillis(),
                report.itemsPerSecond(),
                report.blockedIngressSubmissions(),
                report.ingressHighWatermark());
    }
}
