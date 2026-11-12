resource "fabric_workspace" "this" {
  display_name = var.display_name
  description  = var.description
  capacity_id  = var.capacity_id
}

# The identity running Terraform (the CI service principal) becomes Admin as creator;
# the human administrator is granted Admin explicitly.
resource "fabric_workspace_role_assignment" "admin_user" {
  workspace_id = fabric_workspace.this.id
  role         = "Admin"

  principal = {
    id   = var.admin_user_object_id
    type = "User"
  }
}

resource "fabric_lakehouse" "this" {
  for_each = var.lakehouses

  workspace_id = fabric_workspace.this.id
  display_name = each.key
  description  = each.value

  configuration = {
    enable_schemas = true
  }

  # Lakehouses hold data: never let a plan silently destroy and recreate them.
  lifecycle {
    prevent_destroy = true
  }
}

resource "fabric_eventhouse" "this" {
  workspace_id = fabric_workspace.this.id
  display_name = "eh_bikes"
  description  = "Hot path: station status in KQL (seconds latency) for the Real-Time Dashboard and Activator"

  lifecycle {
    prevent_destroy = true
  }
}

resource "fabric_kql_database" "this" {
  workspace_id = fabric_workspace.this.id
  display_name = "kqldb_bikes"
  description  = "Raw station status, update policy and materialized views (schema script lives in Git)"

  configuration = {
    database_type = "ReadWrite"
    eventhouse_id = fabric_eventhouse.this.id
  }

  lifecycle {
    prevent_destroy = true
  }
}

# Shell only: sources, routing and destinations are a Git-managed definition (Step 4), not Terraform's.
resource "fabric_eventstream" "this" {
  workspace_id = fabric_workspace.this.id
  display_name = "es_station_status"
  description  = "Poller custom endpoint -> Eventhouse (hot) + lh_bronze (warm)"
}

# Medallion Gold tables read in place (no copy) by the Direct Lake model.
resource "fabric_shortcut" "medallion_gold" {
  for_each = toset(var.medallion_gold_tables)

  workspace_id = fabric_workspace.this.id
  item_id      = fabric_lakehouse.this["lh_gold"].id
  path         = "Tables/dbo"
  name         = each.key

  target = {
    onelake = {
      workspace_id = var.medallion_workspace_id
      item_id      = var.medallion_gold_lakehouse_id
      path         = "Tables/dbo/${each.key}"
    }
  }
}

resource "fabric_spark_workspace_settings" "this" {
  workspace_id = fabric_workspace.this.id

  automatic_log = {
    enabled = true
  }

  environment = {
    runtime_version = var.runtime_version
  }

  job = {
    session_timeout_in_minutes = var.session_timeout_minutes
  }

  pool = {
    starter_pool = {
      max_node_count = var.starter_pool_max_nodes
      # one node runs the driver: Fabric rejects max_executors >= max_node_count on update (accepted only on create)
      max_executors = var.starter_pool_max_nodes - 1
    }
  }
}
