# Deploying Insider Alpha Dashboard to GCP

The easiest and most scalable way to deploy this dashboard on Google Cloud Platform is using **Google Cloud Run**. 

Because Cloud Run only exposes a single external port per service, and the Next.js frontend needs to call the FastAPI backend, the recommended architecture is to deploy them as **two separate Cloud Run services** (one for the backend, one for the frontend). They can use the exact same Docker image, but we will override the container command for each.

## Prerequisites

1. Install the [Google Cloud CLI (`gcloud`)](https://cloud.google.com/sdk/docs/install) and authenticate:
   ```bash
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   ```
2. Enable the required GCP APIs:
   ```bash
   gcloud services enable run.googleapis.com \
                          artifactregistry.googleapis.com \
                          cloudbuild.googleapis.com
   ```
3. A Google Cloud Service Account with BigQuery access. See the **Service Account Setup** section below.

---

## Step 1: Service Account Setup

To allow the FastAPI backend to securely query BigQuery without using your personal credentials, you must create a dedicated Service Account and grant it the necessary roles (`BigQuery Data Viewer` and `BigQuery Job User`).

1. **Create the Service Account**:
   ```bash
   gcloud iam service-accounts create insider-alpha-sa \
     --description="Service account for Insider Alpha Dashboard Cloud Run" \
     --display-name="Insider Alpha SA"
   ```

2. **Grant BigQuery Roles**:
   Assign the newly created Service Account the permissions it needs to read your datasets and execute queries. Replace `YOUR_PROJECT_ID` with your actual Google Cloud Project ID.

   ```bash
   # Grant BigQuery Job User (to run queries)
   gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
     --member="serviceAccount:insider-alpha-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/bigquery.jobUser"

   # Grant BigQuery Data Viewer (to read the tables)
   gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
     --member="serviceAccount:insider-alpha-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/bigquery.dataViewer"
   ```

You will use this Service Account email (`insider-alpha-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com`) in the `--service-account` flag when deploying the backend in Step 3.

---

## Step 2: Build the Docker Image on GCP

Submit the existing `Dockerfile` to Cloud Build to build the image and push it to Google Container Registry (or Artifact Registry).

```bash
# Run this from the project root (/visualisation)
gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/insider-dashboard
```

*(Note: If you use Artifact Registry, the tag will look like `REGION-docker.pkg.dev/YOUR_PROJECT_ID/REPO/insider-dashboard`)*

---

## Step 3: Deploy the FastAPI Backend

Deploy the backend service. We will override the container entrypoint to only start `uvicorn` and listen on Cloud Run's `$PORT`.

```bash
gcloud run deploy insider-backend \
  --image gcr.io/YOUR_PROJECT_ID/insider-dashboard \
  --command "bash" \
  --args "-c","cd /app/backend && uv run uvicorn main:app --host 0.0.0.0 --port \$PORT" \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --service-account YOUR_SERVICE_ACCOUNT_EMAIL \
  --set-env-vars GOOGLE_PROJECT_ID=YOUR_PROJECT_ID,BIGQUERY_DATASET=insider_transactions
```

Once deployed, the output will give you the **Service URL** (e.g., `https://insider-backend-xxxxx-uc.a.run.app`).

---

## Step 4: Deploy the Next.js Frontend

Deploy the frontend service. We'll set the `NEXT_PUBLIC_API_URL` environment variable so Next.js knows exactly where to route API requests (the URL from Step 3). We will also override the command to only run Next.js.

```bash
# Replace BACKEND_SERVICE_URL with the URL generated from Step 3
gcloud run deploy insider-frontend \
  --image gcr.io/YOUR_PROJECT_ID/insider-dashboard \
  --command "bash" \
  --args "-c","cd /app/frontend && npm run start" \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars NEXT_PUBLIC_API_URL=https://BACKEND_SERVICE_URL/api
```
*(Note: Ensure `/api` is included at the end of the `NEXT_PUBLIC_API_URL`)*

Once deployed, this command will output the **Frontend Service URL**. You can visit this URL in your browser to view the fully deployed Insider Alpha Dashboard!

---

## Updating the Deployment
Whenever you make changes to the code:
1. Submit a new build:
   `gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/insider-dashboard`
2. Update the services to use the latest image:
   ```bash
   gcloud run services update insider-backend --image gcr.io/YOUR_PROJECT_ID/insider-dashboard
   gcloud run services update insider-frontend --image gcr.io/YOUR_PROJECT_ID/insider-dashboard
   ```
