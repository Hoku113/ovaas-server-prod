# Input bindings are passed in via param block.
param($Timer)

# Get the current universal time in the default string format
$currentUTCtime = (Get-Date).ToUniversalTime()

# The 'IsPastDue' porperty is 'true' when the current function invocation is later than scheduled.
if ($Timer.IsPastDue) {
    Write-Host "PowerShell timer is running late!"
}

New-AzResourceGroup -Name $env:ACI_RESOURCE_GROUP_NAME -Location japaneast
New-AzContainerGroup -ResourceGroupName $env:ACI_RESOURCE_GROUP_NAME -Name $env:ACI_CONTAINER_GROUP_NAME `
    -Image mcr.microsoft.com/azure-cli -OsType Linux `
    -IpAddressType Public `
    -Command "/bin/bash -c ""cd && az login --service-principal --username $env:AZURE_SP_APP_ID --password $env:AZURE_SP_PASSWORD --tenant $env:AZURE_SP_TENANT && git clone https://github.com/OVaaS/ovaas-server-prod.git && cd ovaas-server-prod && source ./deploy_vm.sh deploy_config_singlevm.json $env:OVAAS_OVMS_RESOURCE_GROUP '$env:AZURE_STORAGE_CONNECTION_STRING' && cd""" `
    -RestartPolicy OnFailure
if ($?) {
    $body = "This HTTP triggered function executed successfully. Started container group $env:ACI_CONTAINER_GROUP_NAME"
}
else  {
    $body = "There was a problem starting the container group."
}

# Write an information log with the current time.
Write-Host "PowerShell timer trigger function ran! TIME: $currentUTCtime"
