Param(
    [string]$Uri = "https://localhost:8081/",
    [string]$AuthKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
    [string]$ConsistencyLevel = "BoundedStaleness",
    [string]$BuildVersion = "1.0.0",
    [string]$Configuration = "Release"
)

function Write-Stage([string]$name)
{
    Write-Host $('=' * 30) -ForegroundColor Green
    Write-Host $name -ForegroundColor Green
    Write-Host $('=' * 30) -ForegroundColor Green
}

function Exec
{
    [CmdletBinding()]
    param(
        [Parameter(Position=0,Mandatory=1)][scriptblock]$cmd,
        [Parameter(Position=1,Mandatory=0)][string]$errorMessage = ($msgs.error_bad_command -f $cmd)
    )
    & $cmd
    if ($lastexitcode -ne 0) {
        throw ("Exec: " + $errorMessage)
    }
}

$outputDir = "../../output";
Push-Location src\SimpleEventStore

Write-Stage "Building solution"
Exec { dotnet build -c $Configuration -p:BuildVersion=$BuildVersion }

Write-Stage "Running tests"
$env:Uri = $Uri
$env:AuthKey = $AuthKey
$env:ConsistencyLevel = $ConsistencyLevel

Exec { dotnet test SimpleEventStore.Tests -c $Configuration --no-build --logger trx }
Exec { dotnet test SimpleEventStore.AzureDocumentDb.Tests -c $Configuration --no-build --logger trx }

Write-Stage "Creating nuget packages"
rm "../../output/*.nupkg"
Exec { dotnet pack SimpleEventStore -c $Configuration -o $outputDir -p:BuildVersion=$BuildVersion --no-build }
Exec { dotnet pack SimpleEventStore.AzureDocumentDb.Tests -c $Configuration -o $outputDir  -p:BuildVersion=$BuildVersion --no-build }

Pop-Location