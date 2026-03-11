param(
  [string]$ProjectId = 'prd-2-12-2026',
  [string]$Region = 'us-central1',
  [string]$ServiceName = 'lien-automation',
  [string]$RuntimeServiceAccount = 'lien-automation-v3@prd-2-12-2026.iam.gserviceaccount.com'
)

$ErrorActionPreference = 'Stop'
$gcloud = 'gcloud.cmd'

$secretMap = [ordered]@{
  DATABASE_URL = 'lien-automation-database-url'
  SCHEDULE_RUN_TOKEN = 'lien-automation-schedule-run-token'
  SHEETS_KEY = 'lien-automation-sheets-key'
  SBR_CDP_URL = 'lien-automation-sbr-cdp-url'
}

$serviceJson = & $gcloud run services describe $ServiceName --project $ProjectId --region $Region --format=json | ConvertFrom-Json
$containerEnv = @{}
foreach ($entry in $serviceJson.spec.template.spec.containers[0].env) {
  if ($null -ne $entry.name) {
    $containerEnv[$entry.name] = [string]$entry.value
  }
}

foreach ($envName in $secretMap.Keys) {
  if (-not $containerEnv.ContainsKey($envName) -or [string]::IsNullOrEmpty($containerEnv[$envName])) {
    throw "Cloud Run service is missing a value for $envName"
  }

  $secretName = $secretMap[$envName]
  cmd /c "$gcloud secrets describe $secretName --project $ProjectId 1>nul 2>nul"
  if ($LASTEXITCODE -ne 0) {
    & $gcloud secrets create $secretName --project $ProjectId --replication-policy=automatic | Out-Null
  }

  $tempFile = [System.IO.Path]::GetTempFileName()
  try {
    [System.IO.File]::WriteAllText($tempFile, $containerEnv[$envName], [System.Text.UTF8Encoding]::new($false))
    & $gcloud secrets versions add $secretName --project $ProjectId --data-file=$tempFile | Out-Null
  } finally {
    Remove-Item $tempFile -Force -ErrorAction SilentlyContinue
  }

  & $gcloud secrets add-iam-policy-binding $secretName `
    --project $ProjectId `
    --member "serviceAccount:$RuntimeServiceAccount" `
    --role "roles/secretmanager.secretAccessor" | Out-Null

  Write-Output "secret-ready:$secretName"
}
