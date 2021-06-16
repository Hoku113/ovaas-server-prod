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
    try{
        Write-Host "Starting to remove $env:ACI_RESOURCE_GROUP_NAME..."
        Get-AzContainerInstanceLog -ResourceGroupName $env:ACI_RESOURCE_GROUP_NAME -ContainerGroupName $env:ACI_CONTAINER_GROUP_NAME -ErrorAction Stop | ForEach-Object {If($_ -like "*SUCCESSFULLY_DONE*") { Remove-AzResourceGroup -Name $env:ACI_RESOURCE_GROUP_NAME -Force} else {Write-Host "Not finished yet."} }
        Write-Host "Successfully done."
    }
    catch [Microsoft.Rest.Azure.CloudException] {
        Write-Host ('Error message is ' + $_.Exception.Message)
    }
}

# Associate values to output bindings by calling 'Push-OutputBinding'.
Push-OutputBinding -Name Response -Value ([HttpResponseContext]@{
    StatusCode = [HttpStatusCode]::OK
    Body = $body
})
