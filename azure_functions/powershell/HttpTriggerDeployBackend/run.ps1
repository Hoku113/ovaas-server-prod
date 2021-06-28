using namespace System.Net

# Input bindings are passed in via param block.
param($Request, $TriggerMetadata)

# Write to the Azure Functions log stream.
Write-Host "PowerShell HTTP trigger function processed a request."

# Interact with query parameters or the body of the request.
$name = $Request.Query.Name
if (-not $name) {
    $name = $Request.Body.Name
}

$body = "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response."

if ($name) {
    New-AzResourceGroup -Name $env:ACI_RESOURCE_GROUP_NAME -Location japaneast
    New-AzContainerGroup -ResourceGroupName $env:ACI_RESOURCE_GROUP_NAME -Name $env:ACI_CONTAINER_GROUP_NAME `
        -Image mcr.microsoft.com/azure-cli -OsType Linux `
        -IpAddressType Public `
        -Command "/bin/bash -c ""cd && az login --service-principal --username $env:AZURE_SP_APP_ID --password $env:AZURE_SP_PASSWORD --tenant $env:AZURE_SP_TENANT && git clone https://github.com/OVaaS/ovaas-server-prod.git && cd ovaas-server-prod && source ./deploy_vm.sh deploy_config_singlevm.json $name '$env:AZURE_STORAGE_CONNECTION_STRING' && cd""" `
        -RestartPolicy OnFailure
    if ($?) {
        $body = "This HTTP triggered function executed successfully. Started container group $env:ACI_CONTAINER_GROUP_NAME"
    }
    else  {
        $body = "There was a problem starting the container group."
    }
}

# Associate values to output bindings by calling 'Push-OutputBinding'.
Push-OutputBinding -Name Response -Value ([HttpResponseContext]@{
    StatusCode = [HttpStatusCode]::OK
    Body = $body
})
