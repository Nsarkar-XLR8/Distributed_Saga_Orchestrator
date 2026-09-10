# -----------------------------------------------------------------------------
# Kafka Topics Initialization Script (PowerShell)
# Auto-provisions domain event topics and dead-letter topics (DLT)
# -----------------------------------------------------------------------------

$KafkaContainer = "distributed-kafka"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "⚡ Initializing Kafka Topics in $KafkaContainer..." -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

function Create-KafkaTopic {
    param (
        [string]$TopicName,
        [int]$Partitions = 3,
        [int]$ReplicationFactor = 1
    )

    Write-Host "Creating topic: $TopicName (Partitions: $Partitions, Replication: $ReplicationFactor)..." -ForegroundColor Yellow
    docker exec $KafkaContainer kafka-topics --bootstrap-server localhost:9092 `
        --create --if-not-exists `
        --topic $TopicName `
        --partitions $Partitions `
        --replication-factor $ReplicationFactor
}

# 1. Main Domain Event Topics (3 Partitions)
Create-KafkaTopic -TopicName "order-events" -Partitions 3 -ReplicationFactor 1
Create-KafkaTopic -TopicName "inventory-events" -Partitions 3 -ReplicationFactor 1
Create-KafkaTopic -TopicName "payment-events" -Partitions 3 -ReplicationFactor 1

# 2. Dead Letter Topics (DLT) (1 Partition)
Create-KafkaTopic -TopicName "order-events.DLT" -Partitions 1 -ReplicationFactor 1
Create-KafkaTopic -TopicName "inventory-events.DLT" -Partitions 1 -ReplicationFactor 1
Create-KafkaTopic -TopicName "payment-events.DLT" -Partitions 1 -ReplicationFactor 1

Write-Host "==========================================================" -ForegroundColor Green
Write-Host "✅ All Kafka topics successfully provisioned!" -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green

docker exec $KafkaContainer kafka-topics --bootstrap-server localhost:9092 --list
