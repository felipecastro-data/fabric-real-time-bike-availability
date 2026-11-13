variable "project_name" {
  description = "Short project name, used as the prefix for every Fabric workspace."
  type        = string
  default     = "divvy-rti"
}

variable "medallion_project_name" {
  description = "Prefix of the batch medallion workspaces whose Gold tables are shortcut into lh_gold."
  type        = string
  default     = "divvy-medallion"
}

variable "capacity_id" {
  description = "ID of the Fabric capacity that hosts all workspaces."
  type        = string
}

variable "admin_user_object_id" {
  description = "Entra object ID of the human administrator granted Admin on every workspace and the deployment pipeline."
  type        = string
}

variable "lakehouses" {
  description = "Lakehouses created in every environment. Names are identical across environments so notebooks stay environment-agnostic."
  type        = map(string)
  default = {
    lh_bronze = "Bronze: station status events from the Eventstream and daily station_information snapshots"
    lh_silver = "Silver: validated, deduplicated station availability plus dead-letter"
    lh_gold   = "Gold: 15-min availability fact, SCD2 dim_station, shortcuts to medallion Gold"
  }
}

variable "medallion_gold_tables" {
  description = "Medallion Gold tables exposed in lh_gold through OneLake shortcuts."
  type        = list(string)
  default     = ["fact_trips", "dim_date"]
}

variable "environments" {
  description = "Per-environment settings. Order of the deployment pipeline stages follows `stage_order`."
  type = map(object({
    stage_order             = number
    stage_display_name      = string
    starter_pool_max_nodes  = number
    session_timeout_minutes = number
  }))
  default = {
    dev = {
      stage_order             = 0
      stage_display_name      = "Development"
      starter_pool_max_nodes  = 2
      session_timeout_minutes = 20
    }
    test = {
      stage_order             = 1
      stage_display_name      = "Test"
      starter_pool_max_nodes  = 2
      session_timeout_minutes = 20
    }
    prod = {
      stage_order             = 2
      stage_display_name      = "Production"
      starter_pool_max_nodes  = 4
      session_timeout_minutes = 30
    }
  }

  validation {
    condition     = alltrue([for k in keys(var.environments) : contains(["dev", "test", "prod"], k)])
    error_message = "Environment keys must be dev, test or prod."
  }
}

variable "prod_poller_schedule_enabled" {
  description = "Schedule nb_gbfs_poller and nb_silver_availability hourly and nb_bronze_station_information daily in prod. The notebooks reach prod through the deployment pipeline, so a fresh environment applies with false first, then true for the 7-day prod run."
  type        = bool
  default     = false
}

variable "prod_poller_window" {
  description = "UTC window of the prod streaming run: the prod schedules fire only between these times."
  type = object({
    start = string
    end   = string
  })
  default = {
    start = "2026-11-28T00:00:00Z"
    end   = "2026-12-05T00:00:00Z"
  }
}
