// =============================================================================
// main.bicep
// Contoso Data Platform — Microsoft Fabric supporting Azure infrastructure
//
// Deploys:
//   - Resource group scoped resources
//   - Key Vault (for secrets)
//   - Azure SQL Database (ERP source replica)
//   - Log Analytics Workspace (monitoring)
//   - Storage Account (auxiliary / archive)
//
// Usage:
//   az deployment group create \
//     --resource-group rg-contoso-fabric-dev \
//     --template-file infra/bicep/main.bicep \
//     --parameters @infra/bicep/parameters/dev.parameters.json
// =============================================================================

targetScope = 'resourceGroup'

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

@description('Environment name: dev, test, or prod')
@allowed(['dev', 'test', 'prod'])
param environmentName string

@description('Azure region for all resources')
param location string = resourceGroup().location

@description('Project/workload name prefix')
param projectName string = 'contoso-fabric'

@description('Service principal object ID for Key Vault access policy')
param servicePrincipalObjectId string

@description('SQL Admin AAD group object ID')
param sqlAdminGroupObjectId string

@description('Log retention in days')
param logRetentionDays int = 30

@description('Tags to apply to all resources')
param tags object = {
  project:     'contoso-fabric-data-platform'
  environment: environmentName
  managedBy:   'bicep'
  team:        'data-engineering'
}

// ---------------------------------------------------------------------------
// Variables
// ---------------------------------------------------------------------------

var prefix        = '${projectName}-${environmentName}'
var kvName        = 'kv-${replace(prefix, '-', '')}' // Key Vault names: alphanumeric + dashes, max 24 chars
var sqlServerName = 'sql-${prefix}'
var dbName        = 'erp-source-replica'
var logName       = 'log-${prefix}'
var storageName   = replace('st${projectName}${environmentName}', '-', '')

// ---------------------------------------------------------------------------
// Key Vault
// ---------------------------------------------------------------------------

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name:     kvName
  location: location
  tags:     tags
  properties: {
    sku: {
      family: 'A'
      name:   'standard'
    }
    tenantId:                   subscription().tenantId
    enableRbacAuthorization:    true
    enableSoftDelete:            true
    softDeleteRetentionInDays:  90
    enablePurgeProtection:       environmentName == 'prod' ? true : false
    networkAcls: {
      defaultAction: 'Deny'
      bypass:        'AzureServices'
      virtualNetworkRules: []
      ipRules: []
    }
  }
}

// Grant service principal Key Vault Secrets User role
resource kvRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name:  guid(keyVault.id, servicePrincipalObjectId, 'Key Vault Secrets User')
  scope: keyVault
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6') // Key Vault Secrets User
    principalId:      servicePrincipalObjectId
    principalType:    'ServicePrincipal'
  }
}

// ---------------------------------------------------------------------------
// Log Analytics Workspace
// ---------------------------------------------------------------------------

resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name:     logName
  location: location
  tags:     tags
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: logRetentionDays
    features: {
      enableLogAccessUsingOnlyResourcePermissions: true
    }
    workspaceCapping: {
      dailyQuotaGb: environmentName == 'prod' ? 10 : 2
    }
  }
}

// ---------------------------------------------------------------------------
// Azure SQL Server (ERP source replica)
// ---------------------------------------------------------------------------

resource sqlServer 'Microsoft.Sql/servers@2023-05-01-preview' = {
  name:     sqlServerName
  location: location
  tags:     tags
  properties: {
    administrators: {
      administratorType:         'ActiveDirectory'
      azureADOnlyAuthentication: true
      login:                     'sql-admins'
      sid:                       sqlAdminGroupObjectId
      tenantId:                  subscription().tenantId
    }
    minimalTlsVersion:      '1.2'
    publicNetworkAccess:    environmentName == 'prod' ? 'Disabled' : 'Enabled'
  }
}

resource sqlDatabase 'Microsoft.Sql/servers/databases@2023-05-01-preview' = {
  parent: sqlServer
  name:   dbName
  location: location
  tags:     tags
  sku: {
    name:     environmentName == 'prod' ? 'GP_Gen5_4' : 'GP_Gen5_2'
    tier:     'GeneralPurpose'
    family:   'Gen5'
    capacity: environmentName == 'prod' ? 4 : 2
  }
  properties: {
    collation:                    'Latin1_General_100_CI_AS_KS_WS_SC_UTF8'
    maxSizeBytes:                 environmentName == 'prod' ? 107374182400 : 32212254720 // 100GB : 30GB
    zoneRedundant:                environmentName == 'prod'
    readScale:                    environmentName == 'prod' ? 'Enabled' : 'Disabled'
    autoPauseDelay:               environmentName != 'prod' ? 60 : -1
    minCapacity:                  environmentName != 'prod' ? '0.5' : null
    requestedBackupStorageRedundancy: environmentName == 'prod' ? 'Zone' : 'Local'
  }
}

// ---------------------------------------------------------------------------
// Storage Account (auxiliary)
// ---------------------------------------------------------------------------

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name:     storageName
  location: location
  tags:     tags
  kind:     'StorageV2'
  sku: {
    name: environmentName == 'prod' ? 'Standard_ZRS' : 'Standard_LRS'
  }
  properties: {
    accessTier:              'Hot'
    supportsHttpsTrafficOnly: true
    minimumTlsVersion:       'TLS1_2'
    allowBlobPublicAccess:   false
    allowSharedKeyAccess:    false
    networkAcls: {
      defaultAction: environmentName == 'prod' ? 'Deny' : 'Allow'
      bypass:        'AzureServices'
    }
  }
}

// ---------------------------------------------------------------------------
// Outputs
// ---------------------------------------------------------------------------

output keyVaultName       string = keyVault.name
output keyVaultUri        string = keyVault.properties.vaultUri
output sqlServerFqdn      string = sqlServer.properties.fullyQualifiedDomainName
output logAnalyticsId     string = logAnalytics.id
output storageAccountName string = storageAccount.name
