//! Minimal fixed-width table renderer for human CLI output.
//!
//! No external table dep — columns are space-padded to the widest cell,
//! separated by two spaces, last column unpadded, lines right-trimmed.

/// Render `headers` + `rows` as an aligned plain-text table. Rows shorter
/// than the header count are padded with `"-"`; extra cells are ignored.
pub(crate) fn render_table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let cols = headers.len();
    let mut widths: Vec<usize> = headers.iter().map(|h| h.chars().count()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate().take(cols) {
            widths[i] = widths[i].max(cell.chars().count());
        }
    }

    let mut lines = Vec::with_capacity(rows.len() + 1);
    let header_cells: Vec<String> = headers.iter().map(|s| s.to_string()).collect();
    lines.push(format_cells(&header_cells, &widths));
    for row in rows {
        let mut cells = row.clone();
        cells.resize(cols, "-".to_string());
        lines.push(format_cells(&cells, &widths));
    }
    lines.join("\n")
}

fn format_cells(cells: &[String], widths: &[usize]) -> String {
    let mut out = String::new();
    for (i, width) in widths.iter().enumerate() {
        let cell = cells.get(i).map(String::as_str).unwrap_or("");
        if i + 1 == widths.len() {
            out.push_str(cell);
        } else {
            let pad = width.saturating_sub(cell.chars().count());
            out.push_str(cell);
            out.push_str(&" ".repeat(pad));
            out.push_str("  ");
        }
    }
    out.trim_end().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aligns_columns_to_widest_cell() {
        let out = render_table(
            &["NAME", "STATUS"],
            &[
                vec!["local".into(), "online".into()],
                vec!["desktop-win".into(), "offline".into()],
            ],
        );
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], "NAME         STATUS");
        assert_eq!(lines[1], "local        online");
        assert_eq!(lines[2], "desktop-win  offline");
    }

    #[test]
    fn pads_short_rows_and_trims_trailing_space() {
        let out = render_table(&["A", "B", "C"], &[vec!["x".into()]]);
        assert_eq!(out.lines().nth(1).unwrap(), "x  -  -");
        for line in out.lines() {
            assert_eq!(line, line.trim_end());
        }
    }

    #[test]
    fn empty_rows_still_print_header() {
        let out = render_table(&["A"], &[]);
        assert_eq!(out, "A");
    }
}
