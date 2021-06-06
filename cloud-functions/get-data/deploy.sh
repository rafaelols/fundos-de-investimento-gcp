gcloud functions deploy get-data --entry-point main --region us-east1 \
--runtime python38 --trigger-http --max-instances 5 --timeout 540 --memory 512MB
