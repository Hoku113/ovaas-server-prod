Update-AzFunctionApp -Name ovaas-ps-experiment -ResourceGroupName OVaaS_Experiments -IdentityType SystemAssigned

$SP=(Get-AzADServicePrincipal -DisplayName ovaas-ps-experiment).Id
$RG=(Get-AzResourceGroup -Name OVaaS_Experiments).ResourceId
New-AzRoleAssignment -ObjectId $SP -RoleDefinitionName "Contributor" -Scope $RG
