package dev.pipeline.metasync;

import dev.pipeline.metasync.cli.CliProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@SpringBootApplication
@EnableConfigurationProperties(CliProperties.class)
public class MetadataSyncApplication {

    public static void main(String[] args) {
        String[] effectiveArgs = normalizeArgs(args);
        SpringApplication application = new SpringApplication(MetadataSyncApplication.class);
        boolean cli = isCli(effectiveArgs);
        if (cli) {
            application.setWebApplicationType(WebApplicationType.NONE);
        }
        ConfigurableApplicationContext context = application.run(effectiveArgs);
        if (cli) {
            System.exit(SpringApplication.exit(context));
        }
    }

    /**
     * {@code --cli} is a short alias for {@code --pipeline.cli.enabled=true}.
     * Either form starts a non-web JVM, prints the benchmark, and exits.
     */
    static String[] normalizeArgs(String[] args) {
        if (args == null) {
            return new String[0];
        }
        List<String> copy = new ArrayList<>(Arrays.asList(args));
        if (copy.contains("--cli") && !copy.contains("--pipeline.cli.enabled=true")) {
            copy.add("--pipeline.cli.enabled=true");
        }
        return copy.toArray(String[]::new);
    }

    static boolean isCli(String[] args) {
        if (args == null) {
            return false;
        }
        for (String arg : args) {
            if ("--pipeline.cli.enabled=true".equals(arg) || "--cli".equals(arg)) {
                return true;
            }
        }
        return false;
    }
}
