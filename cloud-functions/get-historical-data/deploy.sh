gcloud functions deploy get-historical-data --entry-point main --region us-east1 \
--runtime python38 --trigger-http --max-instances 5 --timeout 300 --memory 512MB