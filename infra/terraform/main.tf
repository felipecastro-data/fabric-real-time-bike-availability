data "fabric_capacity" "this" {
  id = var.capacity_id

  lifecycle {
    postcondition {
      condition     = self.state == "Active"
      error_message = "The Fabric capacity is not Active (paused or expired trial)."
    }
  }
}

# Medallion Gold per environment (dev -> divvy-medallion-dev, ...), read through OneLake shortcuts.
data "fabric_workspace" "medallion" {
  for_each     = var.environments
  display_name = "${var.medallion_project_name}-${each.key}"
}

data "fabric_lakehouse" "medallion_gold" {
  for_each     = var.environments
  workspace_id = data.fabric_workspace.medallion[each.key].id
  display_name = "lh_gold"
}

module "workspace" {
  source   = "./modules/fabric_rti_stack"
  for_each = var.environments

  display_name                = "${var.project_name}-${each.key}"
  description                 = "Divvy real-time bike availability — ${each.value.stage_display_name} environment (managed by Terraform)"
  capacity_id                 = data.fabric_capacity.this.id
  admin_user_object_id        = var.admin_user_object_id
  lakehouses                  = var.lakehouses
  medallion_workspace_id      = data.fabric_workspace.medallion[each.key].id
  medallion_gold_lakehouse_id = data.fabric_lakehouse.medallion_gold[each.key].id
  medallion_gold_tables       = var.medallion_gold_tables
  starter_pool_max_nodes      = each.value.starter_pool_max_nodes
  session_timeout_minutes     = each.value.session_timeout_minutes
}

locals {
  ordered_envs = [for env in sort([for k, v in var.environments : format("%02d-%s", v.stage_order, k)]) : split("-", env)[1]]
}

# Items must exist in every workspace BEFORE the workspaces are assigned to stages,
# so the deployment pipeline pairs them by name instead of creating duplicates.
resource "fabric_deployment_pipeline" "this" {
  display_name = "${var.project_name}-pipeline"
  description  = "Promotes Fabric items Development -> Test -> Production (managed by Terraform)"

  stages = [
    for env in local.ordered_envs : {
      display_name = var.environments[env].stage_display_name
      description  = "${var.environments[env].stage_display_name} stage"
      is_public    = false
      workspace_id = module.workspace[env].workspace_id
    }
  ]

  depends_on = [module.workspace]
}

resource "fabric_deployment_pipeline_role_assignment" "admin_user" {
  deployment_pipeline_id = fabric_deployment_pipeline.this.id
  role                   = "Admin"

  principal = {
    id   = var.admin_user_object_id
    type = "User"
  }
}

# Hourly poller in prod only (dev/test run bounded sessions). Created by CI as the service principal, so scheduled
# runs execute as the service principal.
data "fabric_notebook" "prod_poller" {
  count = var.prod_poller_schedule_enabled ? 1 : 0

  workspace_id = module.workspace["prod"].workspace_id
  display_name = "nb_gbfs_poller"
}

data "fabric_notebook" "prod_station_information" {
  count = var.prod_poller_schedule_enabled ? 1 : 0

  workspace_id = module.workspace["prod"].workspace_id
  display_name = "nb_bronze_station_information"
}

data "fabric_notebook" "prod_silver_availability" {
  count = var.prod_poller_schedule_enabled ? 1 : 0

  workspace_id = module.workspace["prod"].workspace_id
  display_name = "nb_silver_availability"
}

resource "fabric_item_job_scheduler" "prod_poller_hourly" {
  count = var.prod_poller_schedule_enabled ? 1 : 0

  workspace_id = module.workspace["prod"].workspace_id
  item_id      = data.fabric_notebook.prod_poller[0].id
  job_type     = "RunNotebook"
  enabled      = true

  configuration = {
    type               = "Cron"
    interval           = 60 # each run polls ~55 min, then exits
    local_time_zone_id = "UTC"
    start_date_time    = var.prod_poller_window.start
    end_date_time      = var.prod_poller_window.end
  }
}

resource "fabric_item_job_scheduler" "prod_station_information_daily" {
  count = var.prod_poller_schedule_enabled ? 1 : 0

  workspace_id = module.workspace["prod"].workspace_id
  item_id      = data.fabric_notebook.prod_station_information[0].id
  job_type     = "RunNotebook"
  enabled      = true

  configuration = {
    type               = "Daily"
    times              = ["06:00"]
    local_time_zone_id = "Central Standard Time" # Chicago, where the data comes from
    start_date_time    = var.prod_poller_window.start
    end_date_time      = var.prod_poller_window.end
  }
}

resource "fabric_item_job_scheduler" "prod_silver_availability_hourly" {
  count = var.prod_poller_schedule_enabled ? 1 : 0

  workspace_id = module.workspace["prod"].workspace_id
  item_id      = data.fabric_notebook.prod_silver_availability[0].id
  job_type     = "RunNotebook"
  enabled      = true

  configuration = {
    type               = "Cron"
    interval           = 60 # each run streams 50 min (1-min trigger), then stops before the next run
    local_time_zone_id = "UTC"
    start_date_time    = var.prod_poller_window.start
    end_date_time      = var.prod_poller_window.end
  }
}
