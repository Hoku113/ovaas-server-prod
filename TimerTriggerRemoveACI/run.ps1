# Input bindings are passed in via param block.
param($Timer)

# Get the current universal time in the default string format
$currentUTCtime = (Get-Date).ToUniversalTime()

# The 'IsPastDue' porperty is 'true' when the current function invocation is later than scheduled.
if ($Timer.IsPastDue) {
    Write-Host "PowerShell timer is running late!"
}

try{
    Write-Host "Starting to remove $env:ACI_RESOURCE_GROUP_NAME..."
    Get-AzContainerInstanceLog -ResourceGroupName $env:ACI_RESOURCE_GROUP_NAME -ContainerGroupName $env:ACI_CONTAINER_GROUP_NAME -ErrorAction Stop | ForEach-Object {If($_ -like "*SUCCESSFULLY_DONE*") { Remove-AzResourceGroup -Name $env:ACI_RESOURCE_GROUP_NAME -Force} else {Write-Host "Not finished yet."} }
    Write-Host "Successfully done."
}
catch [Microsoft.Rest.Azure.CloudException] {
    Write-Host ('Error message is ' + $_.Exception.Message)
}

# Write an information log with the current time.
Write-Host "PowerShell timer trigger function ran! TIME: $currentUTCtime"
