provider "aws" {
  region = "us-east-1"  # ou a região que você quiser
}

# ----------------------------------------
# Buckets
# ----------------------------------------

resource "aws_s3_bucket" "raw_bucket" {
  bucket = "mzan-raw-rp-case"

  tags = {
    Environment = "Dev"
    Project     = "RPDataPipeline"
    Layer       = "Bronze"
  }
}

resource "aws_s3_bucket" "curated_bucket" {
  bucket = "mzan-curated-rp-case"

  tags = {
    Environment = "Dev"
    Project     = "RPDataPipeline"
    Layer       = "Silver"
  }
}

resource "aws_s3_bucket" "analytics_bucket" {
  bucket = "mzan-analytics-rp-case"

  tags = {
    Environment = "Dev"
    Project     = "RPDataPipeline"
    Layer       = "Gold"
  }
}

# ----------------------------------------
# Bloqueando acesso público nos buckets
# ----------------------------------------

resource "aws_s3_bucket_public_access_block" "raw_public_access" {
  bucket = aws_s3_bucket.raw_bucket.id

  block_public_acls       = true
  block_public_policy     = false
  ignore_public_acls      = true
  restrict_public_buckets = false
}

resource "aws_s3_bucket_public_access_block" "curated_public_access" {
  bucket = aws_s3_bucket.curated_bucket.id

  block_public_acls       = true
  block_public_policy     = false
  ignore_public_acls      = true
  restrict_public_buckets = false
}

resource "aws_s3_bucket_public_access_block" "analytics_public_access" {
  bucket = aws_s3_bucket.analytics_bucket.id

  block_public_acls       = true
  block_public_policy     = false
  ignore_public_acls      = true
  restrict_public_buckets = false
}

resource "aws_s3_bucket" "airflow_logs" {
  bucket = "airflow-rp-logs"

  tags = {
    Name        = "Airflow Logs Bucket"
    Environment = "Dev"
  }
}

resource "aws_s3_bucket_acl" "airflow_logs_acl" {
  bucket = aws_s3_bucket.airflow_logs.id
  acl    = "private"
}

resource "aws_s3_bucket_public_access_block" "airflow_logs_public_block" {
  bucket = aws_s3_bucket.airflow_logs.id

  block_public_acls       = true
  block_public_policy     = false
  ignore_public_acls      = true
  restrict_public_buckets = false
}

output "bucket_name" {
  value = aws_s3_bucket.airflow_logs.bucket
}
