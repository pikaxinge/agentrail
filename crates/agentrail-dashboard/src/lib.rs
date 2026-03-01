use agentrail_core::Plan;

pub fn generate_dashboard(plan: &Plan) -> String {
    let data = serde_json::json!({
        "project": plan.project,
        "phases": plan.phases,
    });

    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>agentrail dashboard</title></head><body><pre id=\"data\">{}</pre></body></html>",
        data
    )
}
