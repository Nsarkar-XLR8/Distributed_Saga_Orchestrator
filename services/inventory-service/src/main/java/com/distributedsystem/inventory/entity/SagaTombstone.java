package com.distributedsystem.inventory.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.Instant;

@Entity
@Table(name = "saga_tombstones")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SagaTombstone {

    @Id
    @Column(name = "order_id", length = 64)
    private String orderId;

    @Column(nullable = false, length = 128)
    private String reason;

    @Column(name = "created_at", nullable = false)
    @Builder.Default
    private Instant createdAt = Instant.now();
}
