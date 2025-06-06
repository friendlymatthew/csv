use crate::classifier::CsvClassifier;
use crate::classifier::{COMMA_CLASS, NEW_LINE_CLASS, QUOTATION_CLASS};
use crate::grammar::Row;
use crate::u8x16::u8x16;
use std::ops::Range;

#[derive(Debug)]
pub struct CsvReader {
    quotation_bitsets: Vec<u64>,
    comma_bitsets: Vec<u64>,
    new_line_bitsets: Vec<u64>,
}

impl CsvReader {
    pub fn new(data: &[u8]) -> Self {
        let vectors = CsvClassifier::new(data).classify();
        let capacity = vectors.len() / 4 + 1;

        let comma_broadcast = u8x16::broadcast(COMMA_CLASS);
        let new_line_broadcast = u8x16::broadcast(NEW_LINE_CLASS);
        let quotation_broadcast = u8x16::broadcast(QUOTATION_CLASS);

        let mut comma_bitsets = Vec::with_capacity(capacity);
        let mut new_line_bitsets = Vec::with_capacity(capacity);
        let mut quotation_bitsets = Vec::with_capacity(capacity);

        vectors.chunks(4).for_each(|chunk| {
            comma_bitsets.push(build_u64(chunk, comma_broadcast));
            new_line_bitsets.push(build_u64(chunk, new_line_broadcast));
            quotation_bitsets.push(build_u64(chunk, quotation_broadcast));
        });

        Self {
            comma_bitsets,
            new_line_bitsets,
            quotation_bitsets,
        }
    }
    pub fn read(&mut self) -> Vec<Row> {
        let mut rows = Vec::new();
        let mut current_row = Vec::new();

        let mut start = 0;
        let mut end = 0;

        for i in 0..self.quotation_bitsets.len() {
            let valid_quotations = remove_escaped_quotations(self.quotation_bitsets[i]);
            let outside_quotations = !mark_inside_quotations(valid_quotations);

            let mut valid_commas = self.comma_bitsets[i] & outside_quotations;
            let mut valid_new_line = self.new_line_bitsets[i] & outside_quotations;

            // no structual characters exist in this bitset,
            // so we can just advance the end cursor
            if valid_commas == 0 && valid_new_line == 0 {
                end += 64;
                continue;
            }

            loop {
                let first_comma = valid_commas.leading_zeros() as usize;
                let first_new_line = valid_new_line.leading_zeros() as usize;

                let bits_traveled = first_comma.min(first_new_line);

                // there aren't any more structual characters to consider
                // so we just advance the end cursor to the next bitset
                if bits_traveled == 64 {
                    end = (i + 1) * 64;
                    break;
                }

                end += bits_traveled;

                if start < end {
                    current_row.push(Range { start, end });

                    if first_new_line < first_comma {
                        rows.push(Row::from(current_row.clone()));
                        current_row.clear();
                    }
                }

                // consume the structual character
                end += 1;

                valid_commas <<= bits_traveled + 1;
                valid_new_line <<= bits_traveled + 1;

                start = end;
            }
        }

        rows
    }
}

const fn remove_escaped_quotations(q: u64) -> u64 {
    let escaped = q & (q << 1);
    let escaped = escaped | (escaped >> 1);

    q & !escaped
}

/// `mark_inside_quotations` does a parallel xor to mark all bits inbetween a quote pair.
/// Note because of how xor works, the closing quote will be marked as 0. This is fine since
/// we use this to mask commas and new_line in between quote pairs.
#[inline]
const fn mark_inside_quotations(mut x: u64) -> u64 {
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

    fn parse_rows(test: &[u8], expected: Vec<Vec<String>>) {
        let mut reader = CsvReader::new(test);
        let rows = reader
            .read()
            .iter()
            .map(|row| {
                row.fields()
                    .iter()
                    .map(|range| String::from_utf8(test[range.clone()].to_vec()).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert_eq!(rows, expected);
    }

    #[test]
    fn read_basic() {
        let data = b"aaa,bbb,ccc";

        parse_rows(
            data,
            vec![vec![
                "aaa".to_string(),
                "bbb".to_string(),
                "ccc".to_string(),
            ]],
        );
    }

    #[test]
    fn read_basic2() {
        let data = b"aaa,\"bbb\",ccc";

        parse_rows(
            data,
            vec![vec![
                "aaa".to_string(),
                "\"bbb\"".to_string(),
                "ccc".to_string(),
            ]],
        );
    }

    #[test]
    fn read_nested() {
        let data = b"\"aaa,howdy\",\"b\"\"bb\",\"ccc\"";

        parse_rows(
            data,
            vec![vec![
                "\"aaa,howdy\"".to_string(),
                "\"b\"\"bb\"".to_string(),
                "\"ccc\"".to_string(),
            ]],
        );
    }

    #[test]
    fn read_newline_field() {
        let data = b"\"aaa,ho\nwdy\",\"b\"\"bb\",\"ccc\"";

        parse_rows(
            data,
            vec![vec![
                "\"aaa,ho\nwdy\"".to_string(),
                "\"b\"\"bb\"".to_string(),
                "\"ccc\"".to_string(),
            ]],
        );
    }

    #[test]
    fn read_crlf_field() {
        let data = b"\"aaa,ho\r\nwdy\",\"b\"\"bb\",\"ccc\"";

        parse_rows(
            data,
            vec![vec![
                "\"aaa,ho\r\nwdy\"".to_string(),
                "\"b\"\"bb\"".to_string(),
                "\"ccc\"".to_string(),
            ]],
        );
    }

    #[test]
    fn read_taxi_zone_lookup_header() {
        let data = b"\"LocationID\",\"Borough\",\"Zone\",\"service_zone\"\n";

        parse_rows(
            data,
            vec![vec![
                r#""LocationID""#.to_string(),
                r#""Borough""#.to_string(),
                r#""Zone""#.to_string(),
                r#""service_zone""#.to_string(),
            ]],
        );
    }

    #[test]
    fn test_taxi_zone_should_split_two_rows() {
        let data = b"\"LocationID\",\"Borough\",\"Zone\",\"service_zone\"\r\n1,\"EWR\"";

        parse_rows(
            data,
            vec![
                vec![
                    r#""LocationID""#.to_string(),
                    r#""Borough""#.to_string(),
                    r#""Zone""#.to_string(),
                    r#""service_zone""#.to_string(),
                ],
                vec!["1".to_string(), r#""EWR""#.to_string()],
            ],
        );
    }

    #[test]
    fn read_taxi_zone_lookup() {
        let data = r#"
"LocationID","Borough","Zone","service_zone"
1,"EWR","Newark Airport","EWR"
2,"Queens","Jamaica Bay","Boro Zone"
3,"Bronx","Allerton/Pelham Gardens","Boro Zone"
4,"Manhattan","Alphabet City","Yellow Zone"
5,"Staten Island","Arden Heights","Boro Zone"
6,"Staten Island","Arrochar/Fort Wadsworth","Boro Zone"
7,"Queens","Astoria","Boro Zone"
8,"Queens","Astoria Park","Boro Zone"
9,"Queens","Auburndale","Boro Zone"
10,"Queens","Baisley Park","Boro Zone"
11,"Brooklyn","Bath Beach","Boro Zone"
12,"Manhattan","Battery Park","Yellow Zone"
13,"Manhattan","Battery Park City","Yellow Zone"
14,"Brooklyn","Bay Ridge","Boro Zone"
15,"Queens","Bay Terrace/Fort Totten","Boro Zone"
16,"Queens","Bayside","Boro Zone"
17,"Brooklyn","Bedford","Boro Zone"
18,"Bronx","Bedford Park","Boro Zone"
19,"Queens","Bellerose","Boro Zone"
20,"Bronx","Belmont","Boro Zone"
21,"Brooklyn","Bensonhurst East","Boro Zone"
22,"Brooklyn","Bensonhurst West","Boro Zone"
23,"Staten Island","Bloomfield/Emerson Hill","Boro Zone"
24,"Manhattan","Bloomingdale","Yellow Zone"
25,"Brooklyn","Boerum Hill","Boro Zone"
26,"Brooklyn","Borough Park","Boro Zone"
27,"Queens","Breezy Point/Fort Tilden/Riis Beach","Boro Zone"
28,"Queens","Briarwood/Jamaica Hills","Boro Zone"
29,"Brooklyn","Brighton Beach","Boro Zone"
30,"Queens","Broad Channel","Boro Zone"
31,"Bronx","Bronx Park","Boro Zone"
32,"Bronx","Bronxdale","Boro Zone"
33,"Brooklyn","Brooklyn Heights","Boro Zone"
34,"Brooklyn","Brooklyn Navy Yard","Boro Zone"
35,"Brooklyn","Brownsville","Boro Zone"
36,"Brooklyn","Bushwick North","Boro Zone"
37,"Brooklyn","Bushwick South","Boro Zone"
38,"Queens","Cambria Heights","Boro Zone"
39,"Brooklyn","Canarsie","Boro Zone"
40,"Brooklyn","Carroll Gardens","Boro Zone"
41,"Manhattan","Central Harlem","Boro Zone"
42,"Manhattan","Central Harlem North","Boro Zone"
43,"Manhattan","Central Park","Yellow Zone"
44,"Staten Island","Charleston/Tottenville","Boro Zone"
45,"Manhattan","Chinatown","Yellow Zone"
46,"Bronx","City Island","Boro Zone"
47,"Bronx","Claremont/Bathgate","Boro Zone"
48,"Manhattan","Clinton East","Yellow Zone"
49,"Brooklyn","Clinton Hill","Boro Zone"
50,"Manhattan","Clinton West","Yellow Zone"
51,"Bronx","Co-Op City","Boro Zone"
52,"Brooklyn","Cobble Hill","Boro Zone"
53,"Queens","College Point","Boro Zone"
54,"Brooklyn","Columbia Street","Boro Zone"
55,"Brooklyn","Coney Island","Boro Zone"
56,"Queens","Corona","Boro Zone"
57,"Queens","Corona","Boro Zone"
58,"Bronx","Country Club","Boro Zone"
59,"Bronx","Crotona Park","Boro Zone"
60,"Bronx","Crotona Park East","Boro Zone"
61,"Brooklyn","Crown Heights North","Boro Zone"
62,"Brooklyn","Crown Heights South","Boro Zone"
63,"Brooklyn","Cypress Hills","Boro Zone"
64,"Queens","Douglaston","Boro Zone"
65,"Brooklyn","Downtown Brooklyn/MetroTech","Boro Zone"
66,"Brooklyn","DUMBO/Vinegar Hill","Boro Zone"
67,"Brooklyn","Dyker Heights","Boro Zone"
68,"Manhattan","East Chelsea","Yellow Zone"
69,"Bronx","East Concourse/Concourse Village","Boro Zone"
70,"Queens","East Elmhurst","Boro Zone"
71,"Brooklyn","East Flatbush/Farragut","Boro Zone"
72,"Brooklyn","East Flatbush/Remsen Village","Boro Zone"
73,"Queens","East Flushing","Boro Zone"
74,"Manhattan","East Harlem North","Boro Zone"
75,"Manhattan","East Harlem South","Boro Zone"
76,"Brooklyn","East New York","Boro Zone"
77,"Brooklyn","East New York/Pennsylvania Avenue","Boro Zone"
78,"Bronx","East Tremont","Boro Zone"
79,"Manhattan","East Village","Yellow Zone"
80,"Brooklyn","East Williamsburg","Boro Zone"
81,"Bronx","Eastchester","Boro Zone"
82,"Queens","Elmhurst","Boro Zone"
83,"Queens","Elmhurst/Maspeth","Boro Zone"
84,"Staten Island","Eltingville/Annadale/Prince's Bay","Boro Zone"
85,"Brooklyn","Erasmus","Boro Zone"
86,"Queens","Far Rockaway","Boro Zone"
87,"Manhattan","Financial District North","Yellow Zone"
88,"Manhattan","Financial District South","Yellow Zone"
89,"Brooklyn","Flatbush/Ditmas Park","Boro Zone"
90,"Manhattan","Flatiron","Yellow Zone"
91,"Brooklyn","Flatlands","Boro Zone"
92,"Queens","Flushing","Boro Zone"
93,"Queens","Flushing Meadows-Corona Park","Boro Zone"
94,"Bronx","Fordham South","Boro Zone"
95,"Queens","Forest Hills","Boro Zone"
96,"Queens","Forest Park/Highland Park","Boro Zone"
97,"Brooklyn","Fort Greene","Boro Zone"
98,"Queens","Fresh Meadows","Boro Zone"
99,"Staten Island","Freshkills Park","Boro Zone"
100,"Manhattan","Garment District","Yellow Zone"
101,"Queens","Glen Oaks","Boro Zone"
102,"Queens","Glendale","Boro Zone"
103,"Manhattan","Governor's Island/Ellis Island/Liberty Island","Yellow Zone"
104,"Manhattan","Governor's Island/Ellis Island/Liberty Island","Yellow Zone"
105,"Manhattan","Governor's Island/Ellis Island/Liberty Island","Yellow Zone"
106,"Brooklyn","Gowanus","Boro Zone"
107,"Manhattan","Gramercy","Yellow Zone"
108,"Brooklyn","Gravesend","Boro Zone"
109,"Staten Island","Great Kills","Boro Zone"
110,"Staten Island","Great Kills Park","Boro Zone"
111,"Brooklyn","Green-Wood Cemetery","Boro Zone"
112,"Brooklyn","Greenpoint","Boro Zone"
113,"Manhattan","Greenwich Village North","Yellow Zone"
114,"Manhattan","Greenwich Village South","Yellow Zone"
115,"Staten Island","Grymes Hill/Clifton","Boro Zone"
116,"Manhattan","Hamilton Heights","Boro Zone"
117,"Queens","Hammels/Arverne","Boro Zone"
118,"Staten Island","Heartland Village/Todt Hill","Boro Zone"
119,"Bronx","Highbridge","Boro Zone"
120,"Manhattan","Highbridge Park","Boro Zone"
121,"Queens","Hillcrest/Pomonok","Boro Zone"
122,"Queens","Hollis","Boro Zone"
123,"Brooklyn","Homecrest","Boro Zone"
124,"Queens","Howard Beach","Boro Zone"
125,"Manhattan","Hudson Sq","Yellow Zone"
126,"Bronx","Hunts Point","Boro Zone"
127,"Manhattan","Inwood","Boro Zone"
128,"Manhattan","Inwood Hill Park","Boro Zone"
129,"Queens","Jackson Heights","Boro Zone"
130,"Queens","Jamaica","Boro Zone"
131,"Queens","Jamaica Estates","Boro Zone"
132,"Queens","JFK Airport","Airports"
133,"Brooklyn","Kensington","Boro Zone"
134,"Queens","Kew Gardens","Boro Zone"
135,"Queens","Kew Gardens Hills","Boro Zone"
136,"Bronx","Kingsbridge Heights","Boro Zone"
137,"Manhattan","Kips Bay","Yellow Zone"
138,"Queens","LaGuardia Airport","Airports"
139,"Queens","Laurelton","Boro Zone"
140,"Manhattan","Lenox Hill East","Yellow Zone"
141,"Manhattan","Lenox Hill West","Yellow Zone"
142,"Manhattan","Lincoln Square East","Yellow Zone"
143,"Manhattan","Lincoln Square West","Yellow Zone"
144,"Manhattan","Little Italy/NoLiTa","Yellow Zone"
145,"Queens","Long Island City/Hunters Point","Boro Zone"
146,"Queens","Long Island City/Queens Plaza","Boro Zone"
147,"Bronx","Longwood","Boro Zone"
148,"Manhattan","Lower East Side","Yellow Zone"
149,"Brooklyn","Madison","Boro Zone"
150,"Brooklyn","Manhattan Beach","Boro Zone"
151,"Manhattan","Manhattan Valley","Yellow Zone"
152,"Manhattan","Manhattanville","Boro Zone"
153,"Manhattan","Marble Hill","Boro Zone"
154,"Brooklyn","Marine Park/Floyd Bennett Field","Boro Zone"
155,"Brooklyn","Marine Park/Mill Basin","Boro Zone"
156,"Staten Island","Mariners Harbor","Boro Zone"
157,"Queens","Maspeth","Boro Zone"
158,"Manhattan","Meatpacking/West Village West","Yellow Zone"
159,"Bronx","Melrose South","Boro Zone"
160,"Queens","Middle Village","Boro Zone"
161,"Manhattan","Midtown Center","Yellow Zone"
162,"Manhattan","Midtown East","Yellow Zone"
163,"Manhattan","Midtown North","Yellow Zone"
164,"Manhattan","Midtown South","Yellow Zone"
165,"Brooklyn","Midwood","Boro Zone"
166,"Manhattan","Morningside Heights","Boro Zone"
167,"Bronx","Morrisania/Melrose","Boro Zone"
168,"Bronx","Mott Haven/Port Morris","Boro Zone"
169,"Bronx","Mount Hope","Boro Zone"
170,"Manhattan","Murray Hill","Yellow Zone"
171,"Queens","Murray Hill-Queens","Boro Zone"
172,"Staten Island","New Dorp/Midland Beach","Boro Zone"
173,"Queens","North Corona","Boro Zone"
174,"Bronx","Norwood","Boro Zone"
175,"Queens","Oakland Gardens","Boro Zone"
176,"Staten Island","Oakwood","Boro Zone"
177,"Brooklyn","Ocean Hill","Boro Zone"
178,"Brooklyn","Ocean Parkway South","Boro Zone"
179,"Queens","Old Astoria","Boro Zone"
180,"Queens","Ozone Park","Boro Zone"
181,"Brooklyn","Park Slope","Boro Zone"
182,"Bronx","Parkchester","Boro Zone"
183,"Bronx","Pelham Bay","Boro Zone"
184,"Bronx","Pelham Bay Park","Boro Zone"
185,"Bronx","Pelham Parkway","Boro Zone"
186,"Manhattan","Penn Station/Madison Sq West","Yellow Zone"
187,"Staten Island","Port Richmond","Boro Zone"
188,"Brooklyn","Prospect-Lefferts Gardens","Boro Zone"
189,"Brooklyn","Prospect Heights","Boro Zone"
190,"Brooklyn","Prospect Park","Boro Zone"
191,"Queens","Queens Village","Boro Zone"
192,"Queens","Queensboro Hill","Boro Zone"
193,"Queens","Queensbridge/Ravenswood","Boro Zone"
194,"Manhattan","Randalls Island","Yellow Zone"
195,"Brooklyn","Red Hook","Boro Zone"
196,"Queens","Rego Park","Boro Zone"
197,"Queens","Richmond Hill","Boro Zone"
198,"Queens","Ridgewood","Boro Zone"
199,"Bronx","Rikers Island","Boro Zone"
200,"Bronx","Riverdale/North Riverdale/Fieldston","Boro Zone"
201,"Queens","Rockaway Park","Boro Zone"
202,"Manhattan","Roosevelt Island","Boro Zone"
203,"Queens","Rosedale","Boro Zone"
204,"Staten Island","Rossville/Woodrow","Boro Zone"
205,"Queens","Saint Albans","Boro Zone"
206,"Staten Island","Saint George/New Brighton","Boro Zone"
207,"Queens","Saint Michaels Cemetery/Woodside","Boro Zone"
208,"Bronx","Schuylerville/Edgewater Park","Boro Zone"
209,"Manhattan","Seaport","Yellow Zone"
210,"Brooklyn","Sheepshead Bay","Boro Zone"
211,"Manhattan","SoHo","Yellow Zone"
212,"Bronx","Soundview/Bruckner","Boro Zone"
213,"Bronx","Soundview/Castle Hill","Boro Zone"
214,"Staten Island","South Beach/Dongan Hills","Boro Zone"
215,"Queens","South Jamaica","Boro Zone"
216,"Queens","South Ozone Park","Boro Zone"
217,"Brooklyn","South Williamsburg","Boro Zone"
218,"Queens","Springfield Gardens North","Boro Zone"
219,"Queens","Springfield Gardens South","Boro Zone"
220,"Bronx","Spuyten Duyvil/Kingsbridge","Boro Zone"
221,"Staten Island","Stapleton","Boro Zone"
222,"Brooklyn","Starrett City","Boro Zone"
223,"Queens","Steinway","Boro Zone"
224,"Manhattan","Stuy Town/Peter Cooper Village","Yellow Zone"
225,"Brooklyn","Stuyvesant Heights","Boro Zone"
226,"Queens","Sunnyside","Boro Zone"
227,"Brooklyn","Sunset Park East","Boro Zone"
228,"Brooklyn","Sunset Park West","Boro Zone"
229,"Manhattan","Sutton Place/Turtle Bay North","Yellow Zone"
230,"Manhattan","Times Sq/Theatre District","Yellow Zone"
231,"Manhattan","TriBeCa/Civic Center","Yellow Zone"
232,"Manhattan","Two Bridges/Seward Park","Yellow Zone"
233,"Manhattan","UN/Turtle Bay South","Yellow Zone"
234,"Manhattan","Union Sq","Yellow Zone"
235,"Bronx","University Heights/Morris Heights","Boro Zone"
236,"Manhattan","Upper East Side North","Yellow Zone"
237,"Manhattan","Upper East Side South","Yellow Zone"
238,"Manhattan","Upper West Side North","Yellow Zone"
239,"Manhattan","Upper West Side South","Yellow Zone"
240,"Bronx","Van Cortlandt Park","Boro Zone"
241,"Bronx","Van Cortlandt Village","Boro Zone"
242,"Bronx","Van Nest/Morris Park","Boro Zone"
243,"Manhattan","Washington Heights North","Boro Zone"
244,"Manhattan","Washington Heights South","Boro Zone"
245,"Staten Island","West Brighton","Boro Zone"
246,"Manhattan","West Chelsea/Hudson Yards","Yellow Zone"
247,"Bronx","West Concourse","Boro Zone"
248,"Bronx","West Farms/Bronx River","Boro Zone"
249,"Manhattan","West Village","Yellow Zone"
250,"Bronx","Westchester Village/Unionport","Boro Zone"
251,"Staten Island","Westerleigh","Boro Zone"
252,"Queens","Whitestone","Boro Zone"
253,"Queens","Willets Point","Boro Zone"
254,"Bronx","Williamsbridge/Olinville","Boro Zone"
255,"Brooklyn","Williamsburg (North Side)","Boro Zone"
256,"Brooklyn","Williamsburg (South Side)","Boro Zone"
257,"Brooklyn","Windsor Terrace","Boro Zone"
258,"Queens","Woodhaven","Boro Zone"
259,"Bronx","Woodlawn/Wakefield","Boro Zone"
260,"Queens","Woodside","Boro Zone"
261,"Manhattan","World Trade Center","Yellow Zone"
262,"Manhattan","Yorkville East","Yellow Zone"
263,"Manhattan","Yorkville West","Yellow Zone"
264,"Unknown","N/A","N/A"
265,"N/A","Outside of NYC","N/A""#;

        let mut csv = Vec::new();

        for row in CsvReader::new(data.as_bytes()).read() {
            let fields = row
                .fields()
                .iter()
                .map(|field_range| {
                    String::from_utf8(data[field_range.clone()].as_bytes().to_vec()).unwrap()
                })
                .collect::<Vec<_>>();

            csv.push(fields);
        }

        for row in &csv {
            println!("{}\n", row.join("\t"));
        }
    }

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
