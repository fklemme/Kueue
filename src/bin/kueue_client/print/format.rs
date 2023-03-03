use std::cmp::max;
use terminal_size::terminal_size;

/// Returns the terminal's width and height.
pub fn term_size() -> (usize, usize) {
    // Try to detect terminal size
    if let Some(term_size) = terminal_size() {
        (term_size.0 .0 as usize, term_size.1 .0 as usize)
    } else {
        // Default VGA terminal size is: (80, 25)
        // But large size is more useful as it is used by tools like "grep".
        (1_000_000, 1_000)
    }
}

/// Calculate column widths for table-style CLI output based on the column's content.
pub fn get_col_widths(min_col_widths: Vec<usize>, max_col_widths: Vec<usize>) -> Vec<usize> {
    assert!(!min_col_widths.is_empty() && !max_col_widths.is_empty());
    assert!(min_col_widths.len() == max_col_widths.len());

    // Make sure that max >= min col width.
    let max_col_widths: Vec<usize> = max_col_widths
        .iter()
        .zip(min_col_widths.iter())
        .map(|(a, b)| max(a, b).to_owned())
        .collect();

    // Total column space available for assignment.
    let total_col_width_available = term_size().0 - (3 * min_col_widths.len() + 1);

    // Return immediately if we have enough space for every column at max width.
    if max_col_widths.iter().sum::<usize>() <= total_col_width_available {
        return max_col_widths;
    }

    // Also return immediately if there is not even enough space to provide the min col widths.
    if total_col_width_available <= min_col_widths.iter().sum::<usize>() {
        return min_col_widths;
    }

    // Final column widths to be returned later. Start with minimum column widths.
    let mut col_widths = min_col_widths;

    // Grow column widths as long as there is remaining space available.
    let mut remaining_col_width_available =
        total_col_width_available - col_widths.iter().sum::<usize>();
    'outer: while remaining_col_width_available > 0 {
        // Sort columns by width.
        let mut indices: Vec<usize> = (0..col_widths.len()).collect();
        indices.sort_by(|i1, i2| col_widths[*i1].cmp(&col_widths[*i2]));

        // Increment smallest column.
        for index in indices {
            if col_widths[index] < max_col_widths[index] {
                col_widths[index] += 1;
                remaining_col_width_available -= 1;
                continue 'outer;
            }
        }

        // This point should never be reached, because if there were enough
        // space for all columns, we would have returned even before the loop.
        unreachable!();
    }

    // No more space for increments available.
    col_widths
}

pub fn dots_front(text: String, len: usize) -> String {
    if text.len() <= len {
        text
    } else {
        "...".to_string() + &text[(text.len() - (len - 3))..]
    }
}

pub fn dots_back(text: String, len: usize) -> String {
    if text.len() <= len {
        text
    } else {
        text[..(len - 3)].to_owned() + "..."
    }
}