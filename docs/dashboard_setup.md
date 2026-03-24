# Dashboard UI Setup Guide

This guide provides comprehensive instructions for setting up and running the Insider Alpha Dashboard UI, which consists of a FastAPI backend and Next.js frontend.

## Architecture Overview

The dashboard UI follows a modern web architecture:
- **Backend**: FastAPI serving REST APIs with BigQuery data access
- **Frontend**: Next.js React application with real-time data visualization
- **Database**: Google BigQuery as the analytical data warehouse
- **Caching**: Built-in caching layers for performance optimization

## Prerequisites

Before setting up the dashboard, ensure you have:
- Completed the [Pre-setup Guideline](setup.md)
- Successfully run the data pipeline and have data in BigQuery
- Node.js 18+ and npm/yarn installed
- Python environment configured with `uv`

## Backend Setup

### 1. Navigate to Backend Directory

```bash
cd visualisation/backend
```

### 2. Install Dependencies

The backend dependencies are managed through the root `pyproject.toml`. Ensure you're in the project root:

```bash
# From project root
uv sync
```

### 3. Environment Configuration

The backend uses the same `.env` file as the main pipeline. Ensure your BigQuery credentials are properly configured:

```bash
# Verify environment variables
echo $TARGET_BIGQUERY_CREDENTIALS_PATH
echo $GOOGLE_APPLICATION_CREDENTIALS
```

### 4. Start the Backend Server

```bash
# From project root
uv run uvicorn visualisation.backend.main:app --host 0.0.0.0 --port 8000 --reload
```

The backend will be available at `http://localhost:8000`

### 5. Verify Backend API

Test the API endpoints:

```bash
# Health check
curl http://localhost:8000/

# API documentation (Swagger UI)
open http://localhost:8000/docs
```

## Frontend Setup

### 1. Navigate to Frontend Directory

```bash
cd visualisation/frontend
```

### 2. Install Node Dependencies

```bash
npm install
# or
yarn install
# or
pnpm install
```

### 3. Environment Configuration

Create environment variables for the frontend:

```bash
# Create .env.local file
cp .env.example .env.local
```

Configure the following variables in `.env.local`:
```env
NEXT_PUBLIC_API_BASE_URL=http://localhost:8000/api
```

### 4. Start the Frontend Development Server

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
```

The frontend will be available at `http://localhost:3000`

## Dashboard Features

### Core Functionality

The Insider Alpha Dashboard provides:

1. **Insider Transaction Analytics**
   - Real-time insider trading data visualization
   - Filtering by company, insider role, transaction type
   - Historical trend analysis

2. **S&P 500 Integration**
   - Company-specific insider activity
   - Sector-based analysis
   - Market comparison tools

3. **Search and Discovery**
   - Advanced search across insider filings
   - Company ticker lookup
   - Insider name search

4. **Data Visualization**
   - Interactive charts using Lightweight Charts
   - Transaction volume analysis
   - Value trend visualization
   - **Cluster Analysis**: Sector and Size filtering pills
   - **Enhanced UI**: Consistent tab ordering (Clusters, then Detailed Transactions)

### User Interface Features

**Tab Navigation**:
- **Clusters Tab**: Primary view for insider trading cluster analysis
- **Detailed Transactions Tab**: Comprehensive transaction listing
- **Overview Tab**: Market summary and statistics

**Cluster Analysis Enhancements**:
- **Sector Filter**: Pill-based sector selection (same as Detailed Transactions)
- **Size Filter**: Transaction size filtering options
- **Consistent UI**: Unified filtering interface across all views
- **Performance**: Optimized queries using dim_sp500_reporting_owner

### API Endpoints

The backend exposes several key endpoints:

- `GET /api/clusters` - Insider trading cluster analysis with sector and size filtering
- `GET /api/clusters/breakdown` - Detailed cluster breakdown using dim_sp500_reporting_owner
- `GET /api/transactions` - Paginated insider transactions with enhanced filtering
- `GET /api/companies` - S&P 500 company directory
- `GET /api/search/insiders` - Insider search functionality
- `GET /api/search/stocks` - Stock search functionality
- `GET /api/transactions/summary` - Aggregated transaction statistics

## Production Deployment

### Backend Deployment

For production deployment, consider:

1. **Containerization**
```bash
# Build Docker image
docker build -t insider-alpha-backend -f visualisation/backend/Dockerfile .

# Run container
docker run -p 8000:8000 --env-file .env insider-alpha-backend
```

2. **Cloud Deployment**
- Deploy to Google Cloud Run
- Configure Cloud SQL or BigQuery access
- Set up proper IAM roles

### Frontend Deployment

1. **Static Site Deployment**
```bash
# Build for production
npm run build

# Deploy to Vercel, Netlify, or similar
npm run start
```

2. **Environment Variables**
Ensure production environment variables are properly configured:
```env
NEXT_PUBLIC_API_BASE_URL=https://your-backend-url.com/api
```

## Performance Optimization

### Caching Strategy

The dashboard implements multi-level caching:

1. **Backend Caching**
   - In-memory cache for frequently accessed data
   - Search directory pre-warming at startup
   - Default transaction cache

2. **Database Optimization**
   - BigQuery table clustering on `symbol` and `date`
   - Partitioning by year for historical data
   - Optimized query patterns

3. **Frontend Optimization**
   - React.memo for component optimization
   - Lazy loading for large datasets
   - Debounced search inputs

### Monitoring and Logs

1. **Backend Monitoring**
```bash
# View application logs
uvicorn --log-level info

# Monitor API performance
# Access /metrics endpoint for Prometheus metrics
```

2. **Frontend Monitoring**
- Browser DevTools for performance profiling
- Network tab for API request analysis
- React DevTools for component debugging

## Troubleshooting

### Common Issues

1. **Backend Connection Issues**
```bash
# Check BigQuery connection
uv run python -c "from google.cloud import bigquery; client = bigquery.Client(); print('Connected to BigQuery')"

# Verify credentials
gcloud auth application-default login
```

2. **Frontend Build Errors**
```bash
# Clear node modules
rm -rf node_modules package-lock.json
npm install

# Check Node.js version
node --version  # Should be 18+
```

3. **CORS Issues**
Ensure CORS is properly configured in `main.py`:
```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

4. **Data Loading Issues**
- Verify dbt models are built: `uv run dbt build`
- Check BigQuery table existence
- Ensure proper IAM permissions

### Performance Issues

1. **Slow API Responses**
- Check BigQuery query execution plans
- Verify table clustering is applied
- Monitor cache hit rates

2. **Frontend Rendering Issues**
- Implement virtual scrolling for large datasets
- Use React.memo for expensive components
- Optimize bundle size with code splitting

## Development Workflow

### Local Development

1. **Start Backend**
```bash
# Terminal 1
uv run uvicorn visualisation.backend.main:app --reload --host 0.0.0.0 --port 8000
```

2. **Start Frontend**
```bash
# Terminal 2
cd visualisation/frontend
npm run dev
```

3. **Development Tools**
- Backend: http://localhost:8000/docs (Swagger UI)
- Frontend: http://localhost:3000
- BigQuery: Console query interface

### Code Updates

1. **Backend Changes**
- FastAPI auto-reloads with `--reload`
- Update API documentation automatically

2. **Frontend Changes**
- Next.js hot module replacement
- Fast refresh for component updates

### Testing

1. **Backend Testing**
```bash
# Run API tests
uv run pytest visualisation/backend/tests/

# API endpoint testing
curl -X GET "http://localhost:8000/api/transactions?limit=10"
```

2. **Frontend Testing**
```bash
# Run unit tests
npm test

# Run E2E tests
npm run test:e2e
```

## Security Considerations

1. **API Security**
- Implement rate limiting
- Use HTTPS in production
- Validate input parameters
- Secure BigQuery credentials

2. **Frontend Security**
- Content Security Policy headers
- XSS protection
- Secure cookie handling
- Environment variable protection

## Next Steps

After setting up the dashboard:

1. Explore the [API documentation](http://localhost:8000/docs)
2. Browse the frontend interface at `http://localhost:3000`
