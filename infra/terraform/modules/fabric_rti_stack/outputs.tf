output "workspace_id" {
  description = "Workspace ID."
  value       = fabric_workspace.this.id
}

output "lakehouse_ids" {
  description = "Lakehouse IDs keyed by name."
  value       = { for name, lh in fabric_lakehouse.this : name => lh.id }
}

output "eventhouse_id" {
  description = "Eventhouse ID."
  value       = fabric_eventhouse.this.id
}

output "kql_database_id" {
  description = "KQL database ID."
  value       = fabric_kql_database.this.id
}

output "eventstream_id" {
  description = "Eventstream ID."
  value       = fabric_eventstream.this.id
}
