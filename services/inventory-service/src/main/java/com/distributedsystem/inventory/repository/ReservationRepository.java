package com.distributedsystem.inventory.repository;

import com.distributedsystem.inventory.entity.Reservation;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface ReservationRepository extends JpaRepository<Reservation, String> {
    List<Reservation> findByOrderId(String orderId);
    Optional<Reservation> findByOrderIdAndProductId(String orderId, String productId);
}
