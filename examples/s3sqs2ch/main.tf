provider "aws" {
  region = var.region
  profile = var.profile
}

# Create an S3 bucket
resource "aws_s3_bucket" "example_bucket" {
  bucket = var.bucket_name
}

# Create an SQS queue
resource "aws_sqs_queue" "example_queue" {
  name = var.sqs_name
}

# Add an S3 bucket policy to allow S3 to send messages to the SQS queue
resource "aws_sqs_queue_policy" "example_queue_policy" {
  queue_url = aws_sqs_queue.example_queue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = "*"
        Action = "sqs:SendMessage"
        Resource = aws_sqs_queue.example_queue.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = aws_s3_bucket.example_bucket.arn
          }
        }
      }
    ]
  })
}

# Configure S3 bucket notification to send events to the SQS queue
resource "aws_s3_bucket_notification" "example_notification" {
  bucket = aws_s3_bucket.example_bucket.id

  queue {
    queue_arn = aws_sqs_queue.example_queue.arn
    events    = ["s3:ObjectCreated:*"]
  }
}
