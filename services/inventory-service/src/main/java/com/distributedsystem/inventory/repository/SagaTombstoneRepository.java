package com.distributedsystem.inventory.repository;

import com.distributedsystem.inventory.entity.SagaTombstone;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SagaTombstoneRepository extends JpaRepository<SagaTombstone, String> {
}
