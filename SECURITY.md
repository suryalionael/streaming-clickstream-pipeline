# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 1.0.x   | ✅ Active development |

## Reporting a Vulnerability

This project is intended for **local development and portfolio demonstration**.
It is **not designed for production deployment** without additional security hardening.

Known security considerations:

- **Default credentials**: MinIO, Kafka, and Streamlit run without authentication.
  Override `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, and enable SASL/SCRAM for
  Kafka before any non-local deployment.
- **No TLS**: All inter-service communication is plaintext. Configure TLS for
  Kafka, MinIO, and the dashboard in production.
- **No authn/authz**: The dashboard has no login mechanism. Add SSO or reverse-proxy
  authentication for public-facing deployments.

If you discover a security issue, please open a [GitHub Issue](https://github.com/suryalionael/streaming-clickstream-pipeline/issues)
with the label `security` rather than a public pull request.
