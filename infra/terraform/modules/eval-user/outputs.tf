output "eval_user_name" {
  description = "IAM user name for the evaluator"
  value       = aws_iam_user.eval.name
}

output "eval_user_arn" {
  description = "ARN of the evaluator IAM user"
  value       = aws_iam_user.eval.arn
}

output "eval_access_key_id" {
  description = "AWS access key ID for the evaluator — share out-of-band"
  value       = aws_iam_access_key.eval.id
  sensitive   = true
}

output "eval_secret_access_key" {
  description = "AWS secret access key for the evaluator — share out-of-band, rotate after eval"
  value       = aws_iam_access_key.eval.secret
  sensitive   = true
}
