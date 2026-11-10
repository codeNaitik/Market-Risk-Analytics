# Azure footprint for the lakehouse:
#   Resource group -> ADLS Gen2 (bronze/silver/gold in one filesystem)
#                  -> Databricks workspace + access connector (Unity Catalog -> storage)
#                  -> Key Vault for API keys
#                  -> optional Event Hubs (Kafka-compatible) for cloud streaming
#                  -> monthly budget with email alerts
data "azurerm_client_config" "current" {}

resource "random_string" "suffix" {
  length  = 5
  upper   = false
  special = false
}

locals {
  name    = "${var.prefix}-${random_string.suffix.result}"
  sa_name = substr(replace("${var.prefix}lake${random_string.suffix.result}", "-", ""), 0, 24)
}

resource "azurerm_resource_group" "rg" {
  name     = "rg-${local.name}"
  location = var.location
  tags     = var.tags
}

# ---------------------------------------------------------------- data lake
resource "azurerm_storage_account" "lake" {
  name                            = local.sa_name
  resource_group_name             = azurerm_resource_group.rg.name
  location                        = azurerm_resource_group.rg.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  account_kind                    = "StorageV2"
  is_hns_enabled                  = true # hierarchical namespace = ADLS Gen2
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  tags                            = var.tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "lake" {
  name               = "lake"
  storage_account_id = azurerm_storage_account.lake.id
}

# Move bronze files to the cool tier after 30 days: raw history is rarely read.
resource "azurerm_storage_management_policy" "lake" {
  storage_account_id = azurerm_storage_account.lake.id
  rule {
    name    = "bronze-to-cool"
    enabled = true
    filters {
      prefix_match = ["lake/bronze/"]
      blob_types   = ["blockBlob"]
    }
    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than = 30
      }
    }
  }
}

# You (the person running terraform) can read/write the lake with `az login` credentials.
resource "azurerm_role_assignment" "me_lake" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}

# ---------------------------------------------------------------- databricks
resource "azurerm_databricks_workspace" "dbx" {
  name                        = "dbw-${local.name}"
  resource_group_name         = azurerm_resource_group.rg.name
  location                    = azurerm_resource_group.rg.location
  sku                         = var.databricks_sku
  managed_resource_group_name = "rg-${local.name}-dbx-managed"
  tags                        = var.tags
}

# Managed identity Unity Catalog uses to reach the lake (no keys stored anywhere).
resource "azurerm_databricks_access_connector" "uc" {
  name                = "dbac-${local.name}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  identity {
    type = "SystemAssigned"
  }
  tags = var.tags
}

resource "azurerm_role_assignment" "uc_lake" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.uc.identity[0].principal_id
}

# ---------------------------------------------------------------- secrets
resource "azurerm_key_vault" "kv" {
  name                       = substr("kv-${replace(local.name, "-", "")}", 0, 24)
  resource_group_name        = azurerm_resource_group.rg.name
  location                   = azurerm_resource_group.rg.location
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  enable_rbac_authorization  = true
  soft_delete_retention_days = 7
  purge_protection_enabled   = false
  tags                       = var.tags
}

resource "azurerm_role_assignment" "me_kv" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = data.azurerm_client_config.current.object_id
}

# ---------------------------------------------------------------- streaming (optional)
resource "azurerm_eventhub_namespace" "ehns" {
  count               = var.enable_event_hubs ? 1 : 0
  name                = "evhns-${local.name}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "Standard" # Standard tier or higher exposes the Kafka endpoint
  capacity            = 1
  tags                = var.tags
}

resource "azurerm_eventhub" "trades" {
  count             = var.enable_event_hubs ? 1 : 0
  name              = "crypto.trades.raw" # = Kafka topic name
  namespace_id      = azurerm_eventhub_namespace.ehns[0].id
  partition_count   = 3
  message_retention = 7
}

# ---------------------------------------------------------------- cost guardrail
resource "azurerm_consumption_budget_resource_group" "budget" {
  name              = "budget-${local.name}"
  resource_group_id = azurerm_resource_group.rg.id
  amount            = var.monthly_budget
  time_grain        = "Monthly"

  time_period {
    start_date = formatdate("YYYY-MM-01'T'00:00:00Z", timestamp())
  }

  dynamic "notification" {
    for_each = [50, 80, 100]
    content {
      enabled        = true
      threshold      = notification.value
      operator       = "GreaterThanOrEqualTo"
      threshold_type = "Actual"
      contact_emails = [var.alert_email]
    }
  }

  lifecycle {
    ignore_changes = [time_period] # timestamp() changes every plan
  }
}
