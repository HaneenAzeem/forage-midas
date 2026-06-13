package com.jpmc.midascore;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class TaskFourTests {
    static final Logger logger = LoggerFactory.getLogger(TaskFourTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Test
    void task_four_verifier() throws InterruptedException {
        userPopulator.populate();
        String[] transactionLines = fileLoader.loadStrings("/test_data/alskdjfh.fhdjsk");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        Thread.sleep(2000);


        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");
        logger.info("use your debugger to find out what wilbur's balance is after all transactions are processed");
        logger.info("kill this test once you find the answer");
        while (true) {
            try {
                java.lang.reflect.Field[] fields = userPopulator.getClass().getDeclaredFields();
                for (java.lang.reflect.Field field : fields) {
                    if (field.getType().getName().contains("Repository") || field.getType().getName().contains("Conduit")) {
                        field.setAccessible(true);
                        Object repoOrConduit = field.get(userPopulator);

                        if (repoOrConduit.getClass().getName().contains("Conduit")) {
                            java.lang.reflect.Field innerRepo = repoOrConduit.getClass().getDeclaredField("userRepository");
                            innerRepo.setAccessible(true);
                            repoOrConduit = innerRepo.get(repoOrConduit);
                        }

                        Iterable<?> users = (Iterable<?>) repoOrConduit.getClass().getMethod("findAll").invoke(repoOrConduit);
                        for (Object user : users) {
                            String userStr = user.toString();
                            if (userStr.toLowerCase().contains("wilbur")) {
                                System.out.println(">>> FOUND WILBUR RECORD: " + userStr);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println(">>> Standard polling check...");
            }
            Thread.sleep(2000);
        }
    }
}


