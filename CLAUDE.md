# Xpedit Backend - FastAPI + Python 3.12

## Commands
```bash
# Local development
uvicorn main:app --host 0.0.0.0 --port 8004 --reload

# Lint
ruff check . --config ruff.toml

# Tests (94 tests)
pytest tests/ -v --tb=short

# Fix lint issues
ruff check . --config ruff.toml --fix
```

## Deployment
- **Production**: Railway (auto-deploy on push to main)
- **Staging**: Railway (auto-deploy on push to staging)
- URL prod: https://web-production-94783.up.railway.app
- URL staging: https://web-staging-5f41.up.railway.app
- Uses NIXPACKS builder (cmake + g++ for route solvers)

## Architecture
**Monolithic** - All routes in `main.py` (4,631 lines). Not ideal but works.

### Key Files
- `main.py` - ALL endpoints, models, auth, middleware, background jobs
- `optimizer.py` - Route optimization (PyVRP > VROOM > OR-Tools)
- `emails.py` - HTML email templates via Resend
- `tests/conftest.py` - Test fixtures, mock strategy

### Endpoint Groups (50+ endpoints)
| Group | Prefix | Auth | Description |
|-------|--------|------|-------------|
| Routes | `/routes` | User | CRUD + start/complete |
| Stops | `/stops` | User | Complete/fail stops |
| Optimize | `/optimize` | User | PyVRP/VROOM/OR-Tools |
| Drivers | `/drivers` | User | Tracking + location |
| Location | `/location` | User | GPS reporting + history |
| Email | `/email` | User | Delivery notifications |
| Admin | `/admin` | Admin | Users, promo codes, broadcast |
| Company | `/company` | User | Fleet management |
| Referral | `/referral` | User | Referral codes + stats |
| Social | `/social` | Admin | AI content + Twitter/LinkedIn |
| Stripe | `/stripe` | Mixed | Checkout + webhooks |
| Places | `/places` | User | Google autocomplete/directions |
| OCR | `/ocr` | User | Label extraction |
| Health | `/health` | None | Status check |

### Auth Pattern
```python
async def get_current_user(authorization: str = Header(default=None)) -> dict:
    # Decode JWT (HS256 or ES256) -> fetch profile from Supabase
```
- `require_admin()` - role == "admin"
- `require_admin_or_dispatcher()` - role in ("admin", "dispatcher")
- `verify_route_access()` / `verify_stop_access()` / `verify_driver_access()` - ownership checks

### Rate Limiting (in-memory)
- `/admin/*`: 60 req/min
- `/auth/*`, `/promo/redeem`: 20 req/min
- `/places/*`: 30 req/min
- `/optimize`: 10 req/min

### Background Jobs (APScheduler)
- Social scheduler: every 60s (check_scheduled_posts)
- Health check: periodic (periodic_health_check)
- Backup: daily 03:00 UTC (locations, routes, stops, email_log, drivers)
- Cleanup: weekly Sun 04:00 UTC (locations >90d, email_log >180d)

### Optimizer (optimizer.py)
- 3 solvers: PyVRP (primary, 0.22% gap) > VROOM (fallback) > OR-Tools (final fallback)
- 4 strategies: nearest_first, farthest_first, businesses_first, round_trip
- Time window support (morning, afternoon, all-day)
- Haversine distance matrix

## Testing
- 94 tests across 6 files
- All external services mocked (conftest.py sets fake env vars BEFORE importing main)
- ChainableMock for Supabase: `.table().select().eq().execute()`
- No real API calls in CI/CD

```bash
# Run specific test file
pytest tests/test_routes.py -v

# Run single test
pytest tests/test_routes.py::test_create_route -v
```

## Linting (ruff.toml)
- line-length: 120
- target: Python 3.12
- Rules: E, W, F, I (errors, warnings, pyflakes, isort)
- CI: `ruff check . --config ruff.toml`

## Environment Variables (.env)
```
# Required
SUPABASE_URL=https://oantjoyexhscoemzfeae.supabase.co
SUPABASE_KEY=           # anon key
SUPABASE_SERVICE_KEY=   # service role (bypasses RLS)
SUPABASE_JWT_SECRET=    # HS256 verification
RESEND_API_KEY=         # Email service

# Optional
SENTRY_DSN=
GOOGLE_API_KEY=         # Maps, geocoding
GOOGLE_AI_API_KEY=      # Gemini for social AI
STRIPE_SECRET_KEY=
STRIPE_WEBHOOK_SECRET=
RESEND_WEBHOOK_SECRET=
TWITTER_CONSUMER_KEY/SECRET=
TWITTER_ACCESS_TOKEN/SECRET=
LINKEDIN_ACCESS_TOKEN=  # Expires ~every 2 months
```

## Conventions
- Pydantic v2 models for all request/response validation
- CORS: xpedit.es + localhost:3000
- Security headers middleware (X-Content-Type-Options, X-Frame-Options, etc.)
- Always use `supabase_admin` (service_role) for server-side operations that need RLS bypass
- Email sender: info@xpedit.es (Resend, verified domain)
- All new endpoints need: auth, rate limiting, input validation, error handling

## CI/CD (.github/workflows/ci.yml)
- Triggers: push to main/staging, PR to main
- Job 1: ruff lint
- Job 2: pytest (depends on lint passing)
- All env vars faked for CI
