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
                $ItemName.typeProperties.blobPathBeginsWith = $ItemName.typeProperties.blobPathBeginsWith.replace('$$$env_name','dev')                       
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
$property.Value.value = "isdc_nonprod@ds.uhc.com"
}

}
 
$JsonData | ConvertTo-Json -Depth 100  | set-content $ADFArmParamPath
