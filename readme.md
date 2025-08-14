# Rearc Data Quest – BLS Time Series Sync to S3

This repository mirrors the **U.S. Bureau of Labor Statistics** (BLS) time series directory  
`https://download.bls.gov/pub/time.series/`  
to an S3 bucket, keeping it **in sync** (additions, updates, deletions).

Bucket (example): `s3://dataquest-gov-bls-timeseries/bls/`  
Public URL pattern: `https://dataquest-gov-bls-timeseries.s3.amazonaws.com/bls/<subdir>/<filename>`

---

## Features

- Crawls BLS directory **without hard?coded filenames**
- Sends a compliant **User-Agent with contact info** (prevents 403; see ENV below)
- **Rate-limited** with **exponential backoff** for reliability
- Uploads **only new/changed** files (compares `Last-Modified` + `Content-Length`)
- **Deletes orphans** in S3 (files removed at source)
- Objects stored with **SSE?S3 (AES256)** and **HTTPS enforced** (bucket policy)
- Ready for **local runs** and **GitHub Actions** (cron)

---

## Prereqs

- AWS account + S3 bucket (e.g., `dataquest-gov-bls-timeseries` in `us-east-1`)
- IAM user/role with:
  - `s3:ListBucket` on the bucket
  - `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on `bls/*`
- **Do not commit secrets**. For GitHub Actions, use repository **Secrets**.

**Bucket policy (public read for `bls/` only, optional):**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "DenyInsecureTransport",
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:*",
      "Resource": ["arn:aws:s3:::dataquest-gov-bls-timeseries","arn:aws:s3:::dataquest-gov-bls-timeseries/*"],
      "Condition": { "Bool": { "aws:SecureTransport": "false" } } },
    { "Sid": "PublicReadOnlyForBlsPrefix",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::dataquest-gov-bls-timeseries/bls/*" }
  ]
}
