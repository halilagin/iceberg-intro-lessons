# Provider Configuration
provider "aws" {
  region = "us-east-1"
}

# VPC Configuration
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "kafka-flink-vpc"
  }
}

# Subnets for Kafka and Flink
resource "aws_subnet" "kafka_subnets" {
  count             = 3
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index + 1}.0/24"
  availability_zone = "us-east-1${["a", "b", "c"][count.index]}"

  tags = {
    Name = "Kafka Subnet ${count.index + 1}"
  }
}

# Security Group for Kafka
resource "aws_security_group" "kafka_sg" {
  name        = "kafka-security-group"
  description = "Security group for Kafka cluster"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# AWS Managed Kafka Cluster
resource "aws_msk_cluster" "kafka_cluster" {
  cluster_name           = "kafka-cluster"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = "kafka.t3.small"
    ebs_volume_size = 100
    client_subnets  = aws_subnet.kafka_subnets[*].id
    security_groups = [aws_security_group.kafka_sg.id]
  }

  # SASL/SCRAM authentication
  client_authentication {
    sasl {
      scram = true
    }
  }

  tags = {
    Name = "Managed Kafka Cluster"
  }
}

# IAM Role for Flink
resource "aws_iam_role" "flink_role" {
  name = "flink-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "kinesisanalytics.amazonaws.com"
        }
      }
    ]
  })
}

# Flink Application
resource "aws_kinesisanalyticsv2_application" "flink_app" {
  name                   = "kafka-integration-flink-app"
  runtime_environment    = "FLINK-1_13"
  service_execution_role = aws_iam_role.flink_role.arn

  application_configuration {
    flink_application_configuration {
      checkpoint_configuration {
        configuration_type = "DEFAULT"
      }

      monitoring_configuration {
        configuration_type = "DEFAULT"
        log_level          = "INFO"
      }
    }

    vpc_configuration {
      subnet_ids         = aws_subnet.kafka_subnets[*].id
      security_group_ids = [aws_security_group.kafka_sg.id]
    }
  }

  tags = {
    Name = "Kafka Integration Flink Application"
  }
}

# Outputs for reference
output "kafka_bootstrap_brokers" {
  value = aws_msk_cluster.kafka_cluster.bootstrap_brokers
}

output "flink_application_id" {
  value = aws_kinesisanalyticsv2_application.flink_app.id
}

