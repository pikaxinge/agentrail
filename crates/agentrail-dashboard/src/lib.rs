use agentrail_core::Plan;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DashboardSummary {
    pub total: usize,
    pub running: usize,
    pub completed: usize,
    pub failed: usize,
    pub needs_attention: usize,
}

pub fn summarize(plan: &Plan) -> DashboardSummary {
    let mut summary = DashboardSummary {
        total: 0,
        running: 0,
        completed: 0,
        failed: 0,
        needs_attention: 0,
    };

    for phase in &plan.phases {
        for step in &phase.steps {
            summary.total += 1;
            match step.status {
                agentrail_core::StepStatus::Claimed => summary.running += 1,
                agentrail_core::StepStatus::Done | agentrail_core::StepStatus::Skipped => {
                    summary.completed += 1;
                }
                agentrail_core::StepStatus::Rejected => summary.failed += 1,
                agentrail_core::StepStatus::Pending => {}
            }
        }
    }

    summary.needs_attention = summary.failed;
    summary
}

pub fn ascii_safe(input: &str) -> String {
    input
        .chars()
        .map(|c| if c.is_ascii() { c } else { '?' })
        .collect()
}

pub fn generate_dashboard(plan: &Plan) -> String {
    let summary = summarize(plan);
    let mermaid = render_mermaid_dag(plan);
    let markdown = render_markdown_report(plan);
    let project = escape_html(&ascii_safe(&plan.project));

    format!(
        concat!(
            "<!doctype html><html><head><meta charset=\"utf-8\">",
            "<title>Agentrail Dashboard</title>",
            "</head><body>",
            "<h1>Agentrail Dashboard</h1>",
            "<p>Project: {project}</p>",
            "<section id=\"summary\">",
            "<div data-testid=\"summary-total\">{total}</div>",
            "<div data-testid=\"summary-running\">{running}</div>",
            "<div data-testid=\"summary-completed\">{completed}</div>",
            "<div data-testid=\"summary-failed\">{failed}</div>",
            "</section>",
            "<section><h2>Workflow</h2>",
            "<pre class=\"mermaid\">{mermaid}</pre>",
            "</section>",
            "<section><h2>Markdown Report</h2>",
            "<pre id=\"markdown-report\">{markdown}</pre>",
            "</section>",
            "</body></html>"
        ),
        project = project,
        total = summary.total,
        running = summary.running,
        completed = summary.completed,
        failed = summary.failed,
        mermaid = escape_html(&mermaid),
        markdown = escape_html(&markdown),
    )
}

pub fn render_mermaid_dag(plan: &Plan) -> String {
    let mut lines = vec!["graph TD".to_string()];

    for phase in &plan.phases {
        for step in &phase.steps {
            lines.push(format!(
                "  {}[\"{}\"]",
                sanitize_mermaid_id(&step.id),
                escape_mermaid_label(&step.id)
            ));
        }
    }

    for phase in &plan.phases {
        for step in &phase.steps {
            let target = sanitize_mermaid_id(&step.id);
            for dep in &step.depends_on {
                lines.push(format!("  {} --> {}", sanitize_mermaid_id(dep), target));
            }
        }
    }

    lines.join("\n")
}

pub fn render_markdown_report(plan: &Plan) -> String {
    let summary = summarize(plan);
    let project = ascii_safe(&plan.project);
    let mut out = String::new();

    out.push_str("# Agentrail Report\n\n");
    out.push_str(&format!("Project: {}\n\n", project));
    out.push_str(&format!("- Total: {}\n", summary.total));
    out.push_str(&format!("- Running: {}\n", summary.running));
    out.push_str(&format!("- Completed: {}\n", summary.completed));
    out.push_str(&format!("- Failed: {}\n", summary.failed));

    out
}

fn escape_html(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn sanitize_mermaid_id(input: &str) -> String {
    let mut out = String::from("n");

    for byte in input.as_bytes() {
        if byte.is_ascii_alphanumeric() {
            out.push(char::from(*byte));
        } else {
            out.push('_');
            append_hex_byte(&mut out, *byte);
        }
    }

    out
}

fn escape_mermaid_label(input: &str) -> String {
    ascii_safe(input).replace('"', "\\\"")
}

fn append_hex_byte(out: &mut String, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push(char::from(HEX[(byte >> 4) as usize]));
    out.push(char::from(HEX[(byte & 0x0f) as usize]));
}
