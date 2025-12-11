package org.apache.cockpit;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.plugin.datasource.api.plugin.DataSourceProcessorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@EnableAsync
@EnableScheduling
@ServletComponentScan
@EnableTransactionManagement
@SpringBootApplication(scanBasePackages = {"org.apache.cockpit.*"}, exclude = {
        org.springframework.boot.autoconfigure.web.embedded.EmbeddedWebServerFactoryCustomizerAutoConfiguration.class,
        MongoAutoConfiguration.class, MongoDataAutoConfiguration.class
})
@Slf4j

public class Cockpit {
    private static final Logger LOGGER = LoggerFactory.getLogger(Cockpit.class);

    public static void main(String[] args) {
        try {
            SpringApplication sa = new SpringApplication(Cockpit.class);
            sa.run(args);
            LOGGER.info("Cockpit Platform started");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @EventListener
    public void run(ApplicationReadyEvent readyEvent) {
        log.info("Received spring application context ready event will load taskPlugin and write to DB");
        DataSourceProcessorProvider.initialize();
    }
}
