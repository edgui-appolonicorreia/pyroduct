/*
  Indicates the terraform cloud provider: aws
*/
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
  backend "s3" {
    bucket = "terraform-stage"
    key = "tfstate/"
    region  = "us-east-1"
  }

  required_version = ">= 0.14.9"
}

/*
  Define the default region us east 1 - north virginia
*/
provider "aws" {
  profile = "default"
  region  = "us-east-1"
}
