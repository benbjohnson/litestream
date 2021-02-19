[CmdletBinding()]
Param (
    [Parameter(Mandatory = $true)]
    [String] $Version
)
$ErrorActionPreference = "Stop"

# Update working directory.
Push-Location $PSScriptRoot
Trap {
    Pop-Location
}

Invoke-Expression "candle.exe -nologo -arch x64 -ext WixUtilExtension -out litestream.wixobj -dVersion=`"$Version`" litestream.wxs"
Invoke-Expression "light.exe  -nologo -spdb -ext WixUtilExtension -out `"litestream-${Version}.msi`" litestream.wixobj"

Pop-Location
