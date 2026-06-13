package com.jpmc.midascore.repository;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.entity.TransactionRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaConsumer {

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final RestTemplate restTemplate;

    // Inject RestTemplate alongside repositories via Constructor Injection
    public KafkaConsumer(UserRepository userRepository, TransactionRepository transactionRepository, RestTemplate restTemplate) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
    public void receive(String message) {
        try {
            if (message == null || message.trim().isEmpty()) return;

            String[] tokens = message.split("[,\\s]+");
            if (tokens.length < 3) return;

            long senderId = Long.parseLong(tokens[0].trim());
            long recipientId = Long.parseLong(tokens[1].trim());
            float amount = Float.parseFloat(tokens[2].trim());

            UserRecord sender = userRepository.findById(senderId);
            UserRecord recipient = userRepository.findById(recipientId);

            if (sender != null && recipient != null && sender.getBalance() >= amount) {

                // --- TASK 4 INCENTIVE API SERVICE CALL ---
                float incentiveAmount = 0f;
                try {
                    // Re-package tokens back into a standard Transaction POJO model for serialization
                    com.jpmc.midascore.foundation.Transaction payload =
                            new com.jpmc.midascore.foundation.Transaction(senderId, recipientId, amount);

                    String url = "http://localhost:8080/incentive";
                    com.jpmc.midascore.component.Incentive response =
                            restTemplate.postForObject(url, payload, com.jpmc.midascore.component.Incentive.class);

                    if (response != null) {
                        incentiveAmount = response.getAmount();
                    }
                } catch (Exception apiEx) {
                    // Fallback to 0 if API server encounters issues
                    incentiveAmount = 0f;
                }
                // ----------------------------------------

                // Sender loses ONLY the baseline amount
                sender.setBalance(sender.getBalance() - amount);

                // Recipient receives baseline amount PLUS the new incentive amount
                recipient.setBalance(recipient.getBalance() + amount + incentiveAmount);

                // Commit states to H2 tables
                userRepository.save(sender);
                userRepository.save(recipient);

                // Save the base transaction record
                transactionRepository.save(new TransactionRecord(sender, recipient, amount));

                System.out.println("Successfully processed amount: " + amount + " (Incentive: " + incentiveAmount + ")");
            }

        } catch (Exception e) {
            System.err.println("Skipping bad parsing layout line: " + e.getMessage());
        }
    }
}