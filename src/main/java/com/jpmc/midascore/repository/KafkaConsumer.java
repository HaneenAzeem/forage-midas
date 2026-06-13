package com.jpmc.midascore.repository;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.entity.TransactionRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;

    public KafkaConsumer(UserRepository userRepository, TransactionRepository transactionRepository) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
    public void receive(String message) {
        try {
            if (message == null || message.trim().isEmpty()) return;

            // Split file entries by commas or whitespace tokens
            String[] tokens = message.split("[,\\s]+");
            if (tokens.length < 3) return;

            // Map layout assumptions: senderId, recipientId, amount
            long senderId = Long.parseLong(tokens[0].trim());
            long recipientId = Long.parseLong(tokens[1].trim());
            float amount = Float.parseFloat(tokens[2].trim());

            // 1. Database user lookup queries
            UserRecord sender = userRepository.findById(senderId);
            UserRecord recipient = userRepository.findById(recipientId);

            // 2. Validate financial rules conditions
            if (sender != null && recipient != null && sender.getBalance() >= amount) {
                // Adjust cash balances
                sender.setBalance(sender.getBalance() - amount);
                recipient.setBalance(recipient.getBalance() + amount);

                // Commit states to H2 tables
                userRepository.save(sender);
                userRepository.save(recipient);
                transactionRepository.save(new TransactionRecord(sender, recipient, amount));

                System.out.println("Successfully processed amount: " + amount);
            }

        } catch (Exception e) {
            System.err.println("Skipping bad parsing layout line: " + e.getMessage());
        }
    }
}