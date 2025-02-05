use std::ops::Range;

#[derive(Debug, PartialEq)]
pub struct Row(Vec<Range<usize>>);

impl Row {
    pub fn from(fields: Vec<Range<usize>>) -> Self {
        Self(fields)
    }

    pub fn fields(&self) -> &[Range<usize>] {
        &self.0
    }
}
