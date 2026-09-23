# Partner Portal

A lightweight OpenAI-compatible reverse proxy with durable metering and embedded Vue dashboard.

**Status: In Development**

## Overview

Partner Portal is a minimal reverse proxy that:
- Proxies requests to a single OpenAI-compatible upstream
- Performs local API-key authentication with upstream key replacement
- Records usage metrics to SQLite with 60-day retention
- Provides an embedded Vue dashboard for usage inspection
- Supports hot-reload configuration
- Enables zero-downtime rolling updates on a single VPS

## Endpoints

- `POST /v1/chat/completions` - Chat completions (streaming + non-streaming)
- `POST /v1/responses` - Responses API (streaming + non-streaming)
- `GET /v1/models` - List models
- `GET /api/me` - Authenticated consumer info
- `GET /api/dashboard/*` - Dashboard API (authenticated)
- `GET /api/dashboard/events` - SSE realtime updates

## License

Apache-2.0
