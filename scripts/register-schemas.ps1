param(
    [string]$RegistryUrl = "http://localhost:18081"
)

$ErrorActionPreference = "Stop"

python schemas/register_schemas.py --registry $RegistryUrl --proto schemas/copilot_events.proto
