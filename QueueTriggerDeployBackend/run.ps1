# Input bindings are passed in via param block.
param([string] $QueueItem, $TriggerMetadata)

New-AzResourceGroup -Name $env:ACI_RESOURCE_GROUP_NAME -Location japaneast
New-AzContainerGroup -ResourceGroupName $env:ACI_RESOURCE_GROUP_NAME -Name $env:ACI_CONTAINER_GROUP_NAME `
    -Image mcr.microsoft.com/azure-cli -OsType Linux `
    -IpAddressType Public `
    -Command "/bin/bash -c ""cd && az login --service-principal --username $env:AZURE_SP_APP_ID --password $env:AZURE_SP_PASSWORD --tenant $env:AZURE_SP_TENANT && git clone https://github.com/OVaaS/ovaas-server-prod.git && cd ovaas-server-prod && wget -O latest_deploy_config.json $QueueItem && source ./deploy_vm.sh latest_deploy_config.json $env:OVAAS_OVMS_RESOURCE_GROUP '$env:AZURE_STORAGE_CONNECTION_STRING' && cd""" `
    -RestartPolicy OnFailure

if ($?) {
    $body = "This HTTP triggered function executed successfully. Started container group $env:ACI_CONTAINER_GROUP_NAME"
}
else  {
    $body = "There was a problem starting the container group."
}

# Write out the queue message and insertion time to the information log.
Write-Host "PowerShell queue trigger function processed work item: $QueueItem"
Write-Host "Queue item insertion time: $($TriggerMetadata.InsertionTime)"
