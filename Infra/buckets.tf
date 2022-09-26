# S3 Buckets file

/*
    Resource: S3 Bucket
    Name: RSM Bronze layer
    Description: S3 Bucket to receive all raw data.
    Author: Edson G. A. Correia
*/
resource "aws_s3_bucket" "rsm-bronze-layer" {
    bucket = "rsm-bronze-layer"
    tags = {
        environment = "dev"
        project = "data-lake"
    }
}

# S3 Bucket ACL
resource "aws_s3_bucket_acl" "rsm-bronze-layer-acl" {
    bucket = aws_s3_bucket.rsm-bronze-layer.id
    acl = "private"
}

# S3 Bucket versioning
resource "aws_s3_bucket_versioning" "rsm-bronze-layer-versioning" {
    bucket = aws_s3_bucket.rsm-bronze-layer.id
    versioning_configuration {
        status = "Disabled"
    }
}

# Bucket public access block
resource "aws_s3_bucket_public_access_block" "public-block-bronze" {
    bucket = aws_s3_bucket.rsm-bronze-layer.id
    block_public_acls = true
    block_public_policy = true
    restrict_public_buckets = true
    ignore_public_acls = true
}

/*
    Resource: S3 Bucket
    Name: RSM Silver layer
    Description:
    Author: Edson G. A. Correia
*/
resource "aws_s3_bucket" "rsm-silver-layer" {
    bucket = "rsm-silver-layer"
    tags = {
        environment = "dev"
        project = "data-lake"
    }
}

# S3 Bucket ACL
resource "aws_s3_bucket_acl" "rsm-silver-layer-acl" {
    bucket = aws_s3_bucket.rsm-silver-layer.id
    acl = "private"
}

# S3 Bucket versioning
resource "aws_s3_bucket_versioning" "rsm-silver-layer-versioning" {
    bucket = aws_s3_bucket.rsm-silver-layer.id
    versioning_configuration {
        status = "Disabled"
    }
}

# Bucket public access block
resource "aws_s3_bucket_public_access_block" "public-block-silver" {
    bucket = aws_s3_bucket.rsm-silver-layer.id
    block_public_acls = true
    block_public_policy = true
    restrict_public_buckets = true
    ignore_public_acls = true
}

/*
    Resource: S3 Bucket
    Name: RSM Artifacts
    Description:
    Author: Edson G. A. Correia
*/
resource "aws_s3_bucket" "rsm-artifacts" {
    bucket = "rsm-artifacts"
    tags = {
        environment = "dev"
        project = "data-lake"
    }
}

# S3 Bucket ACL
resource "aws_s3_bucket_acl" "rsm-artifacts-acl" {
    bucket = aws_s3_bucket.rsm-artifacts.id
    acl = "private"
}

# S3 Bucket versioning
resource "aws_s3_bucket_versioning" "rsm-artifacts-versioning" {
    bucket = aws_s3_bucket.rsm-artifacts.id
    versioning_configuration {
        status = "Disabled"
    }
}

# Bucket public access block
resource "aws_s3_bucket_public_access_block" "public-block-artifacts" {
    bucket = aws_s3_bucket.rsm-artifacts.id
    block_public_acls = true
    block_public_policy = true
    restrict_public_buckets = true
    ignore_public_acls = true
}
