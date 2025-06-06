use crate::classifier::CsvClassifier;
use crate::classifier::{COMMA_CLASS, NEW_LINE_CLASS, QUOTATION_CLASS};
use crate::grammar::Row;
use crate::u8x16::u8x16;
use anyhow::Result;
use std::ops::Range;

#[derive(Debug)]
pub struct CsvReader {
    quotation_bitsets: Vec<u64>,
    comma_bitsets: Vec<u64>,
    whitespace_bitsets: Vec<u64>,
}

impl CsvReader {
    pub fn new(data: &[u8]) -> Self {
        let vectors = CsvClassifier::new(data).classify();
        let capacity = vectors.len() / 4 + 1;

        let comma_broadcast = u8x16::broadcast(COMMA_CLASS);
        let whitespace_broadcast = u8x16::broadcast(NEW_LINE_CLASS);
        let quotation_broadcast = u8x16::broadcast(QUOTATION_CLASS);

        let mut comma_bitsets = Vec::with_capacity(capacity);
        let mut whitespace_bitsets = Vec::with_capacity(capacity);
        let mut quotation_bitsets = Vec::with_capacity(capacity);

        vectors.chunks(4).into_iter().for_each(|chunk| {
            comma_bitsets.push(build_u64(chunk, comma_broadcast));
            whitespace_bitsets.push(build_u64(chunk, whitespace_broadcast));
            quotation_bitsets.push(build_u64(chunk, quotation_broadcast));
        });

        Self {
            comma_bitsets,
            whitespace_bitsets,
            quotation_bitsets,
        }
    }
    pub fn read(&mut self) -> Result<Vec<Row>> {
        // todo! what happens when a csv is greater than 128 bytes?
        // probably would want something like:

        let mut rows = Vec::new();

        let mut current_row = Vec::new();

        let mut cursor = 0;

        for i in 0..self.quotation_bitsets.len() {
            let valid_quotations = remove_escaped_quotations(self.quotation_bitsets[i]);
            let outside_quotations = !mark_inside_quotations(valid_quotations);

            let mut valid_commas = self.comma_bitsets[i] & outside_quotations;
            let mut valid_whitespace = self.whitespace_bitsets[i] & outside_quotations;

            if valid_commas == 0 && valid_whitespace == 0 {
                continue;
            }

            let mut bitset_cursor = 0;

            loop {
                let first_comma = valid_commas.leading_zeros() as usize;
                let first_whitespace = valid_whitespace.leading_zeros() as usize;

                let bits_traveled = first_comma.min(first_whitespace);

                if bits_traveled == 64 {
                    break;
                }

                current_row.push(Range {
                    start: cursor + bitset_cursor,
                    end: cursor + bitset_cursor + bits_traveled,
                });

                if first_whitespace < first_comma {
                    rows.push(Row::from(current_row.clone()));
                    current_row.clear();
                }

                bitset_cursor += bits_traveled + 1;
                valid_commas <<= bits_traveled + 1;
                valid_whitespace <<= bits_traveled + 1;
            }

            cursor += bitset_cursor;
        }

        Ok(rows)
    }
}

fn remove_escaped_quotations(q: u64) -> u64 {
    let escaped = q & (q << 1);
    let escaped = escaped | (escaped >> 1);

    q & !escaped
}

/// `mark_inside_quotations` does a parallel xor to mark all bits inbetween a quote pair.
/// Note because of how xor works, the closing quote will be marked as 0. This is fine since
/// we use this to mask commas and whitespace in between quote pairs.
#[inline]
fn mark_inside_quotations(mut x: u64) -> u64 {
    x ^= x << 1;
    x ^= x << 2;
    x ^= x << 4;
    x ^= x << 8;
    x ^= x << 16;
    x ^= x << 32;

    x << 1
}

// todo, find a quicker way to do this
#[inline]
fn build_u64(chunks: &[u8x16], broadcast: u8x16) -> u64 {
    let mut packed: u64 = 0;
    for (i, &chunk) in chunks.iter().enumerate() {
        let word = chunk.eq(broadcast).bitset() as u64;
        packed |= word << (48 - i * 16);
    }
    packed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init() -> Result<()> {
        let data = std::fs::read("taxi_zone_lookup.csv")?;

        // todo fix alignment
        // let csv_reader1 = CsvReader::new(&data[..64]);

        let slice63 = &data[..63];

        let csv_reader1 = CsvReader::new(&slice63);

        assert_eq!(csv_reader1.quotation_bitsets.len(), 1);
        assert_eq!(csv_reader1.whitespace_bitsets.len(), 1);
        assert_eq!(csv_reader1.comma_bitsets.len(), 1);

        let mut longer_slice = vec![];
        longer_slice.extend_from_slice(slice63);
        longer_slice.push(0x0A);
        longer_slice.extend_from_slice(&data[63..87]);

        let csv_reader2 = CsvReader::new(&longer_slice);
        assert_eq!(csv_reader2.quotation_bitsets.len(), 2);
        assert_eq!(csv_reader2.whitespace_bitsets.len(), 2);
        assert_eq!(csv_reader2.comma_bitsets.len(), 2);

        assert_eq!(
            csv_reader1.quotation_bitsets[0],
            csv_reader2.quotation_bitsets[0]
        );
        assert_eq!(
            csv_reader1.whitespace_bitsets[0],
            csv_reader2.whitespace_bitsets[0]
        );
        assert_eq!(csv_reader1.comma_bitsets[0], csv_reader2.comma_bitsets[0]);

        Ok(())
    }
    fn check_line(test: &[u8], expected: Vec<Vec<String>>) -> Result<()> {
        let mut reader = CsvReader::new(test);
        let rows = reader
            .read()?
            .iter()
            .map(|row| {
                row.fields()
                    .iter()
                    .map(|range| String::from_utf8(test[range.clone()].to_vec()).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert_eq!(rows, expected);

        Ok(())
    }

    #[test]
    fn read_basic() -> Result<()> {
        let data = b"aaa,bbb,ccc";

        check_line(
            data,
            vec![vec![
                "aaa".to_string(),
                "bbb".to_string(),
                "ccc".to_string(),
            ]],
        )?;

        Ok(())
    }

    #[test]
    fn read_basic2() -> Result<()> {
        let data = b"aaa,\"bbb\",ccc";

        check_line(
            data,
            vec![vec![
                "aaa".to_string(),
                "\"bbb\"".to_string(),
                "ccc".to_string(),
            ]],
        )?;

        Ok(())
    }

    #[test]
    fn read_nested() -> Result<()> {
        let data = b"\"aaa,howdy\",\"b\"\"bb\",\"ccc\"";

        check_line(
            data,
            vec![vec![
                "\"aaa,howdy\"".to_string(),
                "\"b\"\"bb\"".to_string(),
                "\"ccc\"".to_string(),
            ]],
        )?;

        Ok(())
    }

    #[test]
    fn read_newline_field() -> Result<()> {
        let data = b"\"aaa,ho\nwdy\",\"b\"\"bb\",\"ccc\"";

        check_line(
            data,
            vec![vec![
                "\"aaa,ho\nwdy\"".to_string(),
                "\"b\"\"bb\"".to_string(),
                "\"ccc\"".to_string(),
            ]],
        )?;

        Ok(())
    }

    #[test]
    fn read_crlf_field() -> Result<()> {
        let data = b"\"aaa,ho\r\nwdy\",\"b\"\"bb\",\"ccc\"";

        check_line(
            data,
            vec![vec![
                "\"aaa,ho\r\nwdy\"".to_string(),
                "\"b\"\"bb\"".to_string(),
                "\"ccc\"".to_string(),
            ]],
        )?;

        Ok(())
    }

    #[test]
    fn read_taxi_zone_lookup_header() -> Result<()> {
        let data = b"\"LocationID\",\"Borough\",\"Zone\",\"service_zone\"\n";

        check_line(
            data,
            vec![vec![
                r#""LocationID""#.to_string(),
                r#""Borough""#.to_string(),
                r#""Zone""#.to_string(),
                r#""service_zone""#.to_string(),
            ]],
        )?;

        Ok(())
    }

    // #[test]
    // fn read_taxi_zone_lookup() -> Result<()> {
    //     let data = std::fs::read("taxi_zone_lookup.csv")?;
    //
    //     // dbg!(String::from_utf8(data[..66].to_vec()).unwrap());
    //
    //     let rows = CsvReader::new(&data).read()?;
    //     dbg!(&rows);
    //
    //     for row in CsvReader::new(&data).read()? {
    //         let fields = row
    //             .fields()
    //             .into_iter()
    //             .map(|field_range| String::from_utf8(data[field_range.clone()].to_vec()).unwrap())
    //             .collect::<Vec<_>>();
    //
    //         dbg!(fields);
    //     }
    //
    //     Ok(())
    // }

    #[test]
    fn test_mark_inside_quotations() {
        let res = mark_inside_quotations(0b10001000);
        assert_eq!(res, 0b11110000);

        let res2 = mark_inside_quotations(0b1001_1001);
        assert_eq!(res2, 0b1110_1110);

        let res4 = mark_inside_quotations(0b1100_1001);
        assert_eq!(res4, 0b1000_1110);
    }
}
