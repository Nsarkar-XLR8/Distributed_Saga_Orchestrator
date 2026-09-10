package com.distributedsystem.inventory.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.Formula;

@Entity
@Table(name = "products")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Product {

    @Id
    @Column(length = 64)
    private String id;

    @Column(nullable = false, unique = true, length = 64)
    private String sku;

    @Column(nullable = false)
    private String name;

    @Column(name = "total_stock", nullable = false)
    private Integer totalStock;

    @Column(name = "reserved_stock", nullable = false)
    private Integer reservedStock;

    @Formula("total_stock - reserved_stock")
    private Integer availableStock;
}
