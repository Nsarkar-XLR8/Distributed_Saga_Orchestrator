package com.distributedsystem.inventory.repository;

import com.distributedsystem.inventory.entity.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {

    @Query(value = """
        UPDATE outbox_events
        SET status = 'IN_FLIGHT', lease_expires_at = NOW() + INTERVAL '10 seconds'
        WHERE id IN (
          SELECT id FROM outbox_events
          WHERE (status = 'PENDING' OR (status = 'IN_FLIGHT' AND lease_expires_at < NOW()))
          ORDER BY created_at ASC
          LIMIT :batchSize
          FOR UPDATE SKIP LOCKED
        )
        RETURNING *
        """, nativeQuery = true)
    List<OutboxEvent> claimLeasedBatch(@Param("batchSize") int batchSize);
}
