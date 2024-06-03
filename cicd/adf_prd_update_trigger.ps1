param(
[String] $ADFArmPath, 
[String] $ADFArmParamPath
)
# <Update Trigger Environment Name>


$JsonData = (Get-Content $ADFArmPath -raw | ConvertFrom-Json)
$JsonData.update | % { if($JsonData.resources.properties)
    {
        foreach ($ItemName in $JsonData.resources.properties ) {
                if($ItemName.type -eq "BlobEventsTrigger")
                {
                $ItemName.typeProperties.blobPathBeginsWith = $ItemName.typeProperties.blobPathBeginsWith.replace('$$$env_name','prd')                       
                    }
                }
            }
    }
    
$JsonData | ConvertTo-Json -Depth 100  | set-content $ADFArmPath


# <Update Trigger Email>

$JsonData = (Get-Content $ADFArmParamPath -raw | ConvertFrom-Json)

foreach ($property in $JsonData.parameters.PSObject.Properties)
{
if ($property.Name -like "tr_wf_*_properties_wf_*_parameters_p_email_recipient")
{
$property.Value.value = "isdc_prod@ds.uhc.com"
}

}
 
$JsonData | ConvertTo-Json -Depth 100  | set-content $ADFArmParamPath



# <Update Trigger Scope>

$JsonData = (Get-Content $ADFArmParamPath -raw | ConvertFrom-Json)

foreach ($property in $JsonData.parameters.PSObject.Properties)
{
if ($property.Name -like "tr_wf_*_properties_typeProperties_scope")
{
$property.Value.value = "/subscriptions/3d58a242-5793-4087-947a-0ca97fbe8fb6/resourceGroups/rg-isdc-prod/providers/Microsoft.Storage/storageAccounts/saisdcprod"
}

}
 
$JsonData | ConvertTo-Json -Depth 100  | set-content $ADFArmParamPath
