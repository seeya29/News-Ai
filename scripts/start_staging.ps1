$env:JWT_SECRET = "staging_secret_key_12345"
$env:LOG_LEVEL = "INFO"
$env:PYTHONPATH = "."

Write-Host "--- News-Ai Staging Deployment ---"
Write-Host "Base URL: http://localhost:8000"
Write-Host "Auth Method: Bearer Token"
Write-Host "Latency Behaviour: ~20s/batch (TTS bottleneck)"

# Generate a token
try {
    $token = python scripts/dev_jwt.py staging_user admin --ttl 86400
    Write-Host "Generated Staging Token (valid 24h):"
    Write-Host $token
    Write-Host ""
} catch {
    Write-Host "Failed to generate token. Ensure python is in PATH and dependencies are installed."
}

Write-Host "Starting Server..."
python -m uvicorn server.app:APP --port 8000 --host 0.0.0.0
