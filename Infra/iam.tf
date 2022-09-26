/*
    Resource: IAM Policy
    Name: datalake_policy
    Description: Gives access to s3, lambda and rds resources.
    Author: Edson G. A. Correia
*/
resource "aws_iam_role_policy" "datalake-policy" {
  name = "datalake_policy"
  role = aws_iam_role.datalake-role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:*",
          "ec2:*",
          "lambda:*",
          "rds:*",
          "logs:*"
        ]
        Effect = "Allow"
        Resource = "*"
      },
    ]
  })
}

/*
  Resource: IAM Role
  Name: datalake_role
  Description: datalake role.
  Author: Edson G. A. Correia
*/
resource "aws_iam_role" "datalake-role" {
  name = "datalake_role"
  description = "Role dedicated to support all actions performed by datalake"
  assume_role_policy = data.aws_iam_policy_document.datalake-role-policy-document.json
  tags = {
    environment = "dev"
    project = "data-lake"
  }
}

/*
  Resource: IAM Policy Document
  Name: datalake-role-policy-document
  Description: datalake role policy document, to be used as assumed_role_policy parameter.
  Author: Edson G. A. Correia
*/
data "aws_iam_policy_document" "datalake-role-policy-document" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type = "Service"
      identifiers = ["lambda.amazonaws.com", "rds.amazonaws.com", "s3.amazonaws.com"]
    }
  }
}

/*
  Resource: Group
  Name: datalake-users-group
  Description: datalake users group.
  Author: Edson G. A. Correia
*/
resource "aws_iam_group" "group-datalake" {
  name = "datalake"
}

/*
*/
resource "aws_iam_group_policy" "my_developer_policy" {
  name  = "my_developer_policy"
  group = aws_iam_group.group-datalake.name

  # Terraform's "jsonencode" function converts a
  # Terraform expression result to valid JSON syntax.
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:*",
          "ec2:*",
          "lambda:*",
          "rds:*",
          "logs:*"
        ]
        Effect = "Allow"
        Resource = "*"
      },
    ]
  })
}

/*
  Resource: Group Membership
  Name: datalake-membership
  Description: datalake users group.
  Author: Edson G. A. Correia
*/
resource "aws_iam_group_membership" "datalake-membership" {
  name = "datalake-membership"
  users = [
    aws_iam_user.datalake-airflow.name
  ]
  group = aws_iam_group.group-datalake.name
  depends_on = [aws_iam_group.group-datalake, aws_iam_user.datalake-airflow]
}

/*
  Resource: User Policy
  Name: datalake-Airflow-user-policy
  Description: datalake user policy.
  Author: Edson G. A. Correia
*/
resource "aws_iam_user_policy" "datalake-airflow-user-policy" {
  name = "datalake-airflow-user-policy"
  user = aws_iam_user.datalake-airflow.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:*",
          "ec2:*",
          "lambda:*",
          "rds:*",
          "logs:*"
        ]
        Effect = "Allow"
        Resource = "*"
      },
    ]
  })
}

/*
  Resource: User
  Name: datalake-Airflow
  Description: datalake user.
  Author: Edson G. A. Correia
*/
resource "aws_iam_user" "datalake-airflow" {
  name = "datalake-airflow"
  force_destroy = true
  depends_on = [aws_iam_role.datalake-role, aws_iam_group.group-datalake]
  tags = {
    environment = "dev"
    project = "data-lake"
  }
}

/*
  Resource: User Access key
  Name: datalake-Airflow
  Description: datalake user's access.
  Author: Edson G. A. Correia
*/
resource "aws_iam_access_key" "datalake-access" {
  user = aws_iam_user.datalake-airflow.name
}

# Exporting values
output "datalake-user-id" {
  value = aws_iam_access_key.datalake-access.id
  sensitive = true
}
output "datalake-user-secret" {
  value = aws_iam_access_key.datalake-access.secret
  sensitive = true
}
