# Input bindings are passed in via param block.
param($Timer)

# Get the current universal time in the default string format
$currentUTCtime = (Get-Date).ToUniversalTime()

# The 'IsPastDue' porperty is 'true' when the current function invocation is later than scheduled.
if ($Timer.IsPastDue) {
    Write-Host "PowerShell timer is running late!"
}

try{
    Write-Host "Starting to remove $env:OVAAS_OVMS_RESOURCE_GROUP..."
    $result = Remove-AzResourceGroup -Name $env:OVAAS_OVMS_RESOURCE_GROUP -Force
    Write-Host "Successfully done."
    Write-Host $result
}
catch [Microsoft.Rest.Azure.CloudException] {
    Write-Host ('Error message is ' + $_.Exception.Message)
}

# Write an information log with the current time.
Write-Host "PowerShell timer trigger function ran! TIME: $currentUTCtime"
