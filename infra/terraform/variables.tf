variable "subscription_id" {
  description = "Azure subscription to deploy into (az account show --query id -o tsv)"
  type        = string
}

variable "prefix" {
  description = "Short name used in every resource name"
  type        = string
  default     = "mra"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "canadacentral"
}

variable "monthly_budget" {
  description = "Monthly cost budget in your billing currency; alerts at 50/80/100%"
  type        = number
  default     = 30
}

variable "alert_email" {
  description = "Where budget alerts are sent"
  type        = string
}

variable "databricks_sku" {
  description = "premium is required for Unity Catalog; trial gives 14 free days of DBUs"
  type        = string
  default     = "premium"
}

variable "enable_event_hubs" {
  description = "Create an Event Hubs namespace (Kafka endpoint) for streaming in the cloud. Costs money while it exists."
  type        = bool
  default     = false
}

variable "tags" {
  type = map(string)

  default = {
    project = "market-risk-analytics"
    owner   = "portfolio"
    env     = "dev"
  }
}
