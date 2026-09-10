package com.distributedsystem.inventory.controller;

import com.distributedsystem.inventory.entity.OutboxEvent;
import com.distributedsystem.inventory.entity.Product;
import com.distributedsystem.inventory.entity.Reservation;
import com.distributedsystem.inventory.repository.OutboxEventRepository;
import com.distributedsystem.inventory.repository.ProductRepository;
import com.distributedsystem.inventory.repository.ReservationRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/inventory")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
public class InventoryController {

    private final ProductRepository productRepository;
    private final ReservationRepository reservationRepository;
    private final OutboxEventRepository outboxEventRepository;

    @GetMapping("/products")
    public ResponseEntity<List<Product>> getAllProducts() {
        return ResponseEntity.ok(productRepository.findAll());
    }

    @GetMapping("/products/{id}")
    public ResponseEntity<Product> getProductById(@PathVariable("id") String id) {
        if (id == null || id.isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        return productRepository.findById(id)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/reservations")
    public ResponseEntity<List<Reservation>> getAllReservations() {
        return ResponseEntity.ok(reservationRepository.findAll());
    }

    @GetMapping("/outbox")
    public ResponseEntity<List<OutboxEvent>> getOutboxEvents() {
        return ResponseEntity.ok(outboxEventRepository.findAll());
    }
}
