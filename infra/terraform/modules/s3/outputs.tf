output "bronze_bucket_name" {
  description = "Name of the S3 bronze bucket"
  value       = aws_s3_bucket.bronze.bucket
}

output "bronze_bucket_arn" {
  description = "ARN of the S3 bronze bucket"
  value       = aws_s3_bucket.bronze.arn
}

output "bronze_bucket_id" {
  description = "ID (name) of the S3 bronze bucket"
  value       = aws_s3_bucket.bronze.id
}

output "athena_workgroup_name" {
  description = "Name of the Athena workgroup for Bronze ad-hoc queries"
  value       = aws_athena_workgroup.bronze_adhoc.name
}
