output "resource_group" {
  value = azurerm_resource_group.rg.name
}

output "lake_root" {
  description = "Put this in .env as LAKE_ROOT"
  value       = "abfss://${azurerm_storage_data_lake_gen2_filesystem.lake.name}@${azurerm_storage_account.lake.name}.dfs.core.windows.net"
}

output "storage_account_name" {
  value = azurerm_storage_account.lake.name
}

output "databricks_workspace_url" {
  value = "https://${azurerm_databricks_workspace.dbx.workspace_url}"
}

output "databricks_access_connector_id" {
  description = "Use when creating the Unity Catalog storage credential"
  value       = azurerm_databricks_access_connector.uc.id
}

output "key_vault_uri" {
  value = azurerm_key_vault.kv.vault_uri
}

output "event_hubs_kafka_bootstrap" {
  value = var.enable_event_hubs ? "${azurerm_eventhub_namespace.ehns[0].name}.servicebus.windows.net:9093" : null
}
