# This script is based on https://learn.microsoft.com/en-us/azure/event-grid/scripts/event-grid-powershell-webhook-secure-delivery-azure-ad-app

# Invoke: setup-eventgrid-sp.ps1 "webhook app ID"
$webhookAppId = $args[0]

# Start execution
try {
    # Creates an application role of given name and description
    function CreateAppRole([string] $Name, [string] $Description)
    {
        $appRole = New-Object Microsoft.Graph.PowerShell.Models.MicrosoftGraphAppRole
        $appRole.AllowedMemberTypes = @("Application", "User")
        $appRole.DisplayName = $Name
        $appRole.Id = New-Guid
        $appRole.IsEnabled = $true
        $appRole.Description = $Description
        $appRole.Value = $Name
        return $appRole
    }

    # Creates Azure Event Grid Azure AD Application if not exists
    # Note: eventGridAppId and eventGridRoleName are constants
    $eventGridAppId = "4962773b-9cdb-44cf-a8bf-237846a00ab7"
    $eventGridRoleName = "AzureEventGridSecureWebhookSubscriber"
    $eventGridSP = Get-MgServicePrincipal -Filter ("appId eq '" + $eventGridAppId + "'")
    if ($eventGridSP.AppId -eq $eventGridAppId)
    {
        Write-Host "The Azure AD Application is already defined.`n"
    } else {
        Write-Host "Creating the Azure Event Grid Azure AD Application"
        $eventGridSP = New-MgServicePrincipal -AppId $eventGridAppId
    }

    # Creates the Azure app role for the webhook Azure AD application
    $app = Get-MgApplication -Filter ("appId eq '" + $webhookAppId + "'")
    [System.Collections.ArrayList]$appRoles = $app.AppRoles

    Write-Host "Azure AD App roles before addition of the new role..."
    Write-Host $appRoles

    if ( $approles  | ?{$_.DisplayName -in "AzureEventGridSecureWebhookSubscriber"} )
    {
        Write-Host "The Azure Event Grid role is already defined."
    } else {
        Write-Host "Creating the Azure Event Grid role in Azure AD Application: " $webhookAppId
        $newRole = CreateAppRole -Name $eventGridRoleName -Description "Azure Event Grid Role"
        $appRoles.Add($newRole)
        Update-MgApplication -ApplicationId $app.Id -AppRoles $appRoles
    }

    Write-Host "Azure AD App roles after addition of the new role..."
    Write-Host $appRoles

    # Creates the user role assignment for the app that will create event subscription
    $servicePrincipal = Get-MgServicePrincipal -Filter ("appId eq '" + $app.AppId + "'")
    try
    {
        Write-Host "Creating the Azure AD Application role assignment: " $webhookAppId
        $eventGridAppRole = $appRoles | Where-Object -Property "DisplayName" -eq -Value $eventGridRoleName
        New-MgServicePrincipalAppRoleAssignment -AppRoleId $eventGridAppRole.Id -ResourceId $servicePrincipal.Id -ServicePrincipalId $servicePrincipal.Id -PrincipalId $servicePrincipal.Id -ErrorAction Stop
    }
    catch
    {
        if( $_.Exception.Message -like '*Permission being assigned already exists on the object*')
        {
            Write-Host "The Azure AD Application role is already defined."
        } else {
            Write-Error $_.Exception.Message
        }
    }

    # Creates the service app role assignment for the Event Grid Azure AD Application
    try
    {
        $eventGridAppRole = $appRoles | Where-Object -Property "DisplayName" -eq -Value $eventGridRoleName
        New-MgServicePrincipalAppRoleAssignment -AppRoleId $eventGridAppRole.Id -ResourceId $servicePrincipal.Id -ServicePrincipalId $eventGridSP.Id -PrincipalId $eventGridSP.Id -ErrorAction Stop
    }
    catch
    {
        if( $_.Exception.Message -like '*Permission being assigned already exists on the object*')
        {
            Write-Host "The Azure AD Application role for the Event Grid Application is already defined."
        } else {
            Write-Error $_.Exception.Message
        }
    }
}
catch {
    Write-Host ">> Exception:"
    Write-Host $_
    Write-Host ">> StackTrace:"  
    Write-Host $_.ScriptStackTrace
}