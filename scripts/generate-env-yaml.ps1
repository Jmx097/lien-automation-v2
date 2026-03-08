# scripts/generate-env-yaml.ps1
# Converts .env to env-vars.yaml for use with gcloud run deploy --env-vars-file
Get-Content .env | Where-Object { $_ -match "^\s*[^#]" -and $_ -match "=" } | ForEach-Object {
    $key = $_.Split("=")[0].Trim()
    $value = $_.Substring($_.IndexOf("=")+1).Trim()
    "$key`: '$value'"
} | Set-Content env-vars.yaml

Write-Host "env-vars.yaml generated. Deploy with:"
Write-Host "gcloud run deploy lien-automation --source . --region us-central1 --project prd-2-12-2026 --service-account lien-automation-v3@prd-2-12-2026.iam.gserviceaccount.com --allow-unauthenticated --port 8080 --memory 2Gi --cpu 2 --timeout 3600 --env-vars-file env-vars.yaml"
Write-Host "Then delete: Remove-Item env-vars.yaml"
