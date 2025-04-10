use std::{fs::OpenOptions, net::Ipv4Addr};

use anyhow::Result;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct Record {
    #[serde(rename = "IP开始")]
    ip_start: Ipv4Addr,
    #[serde(rename = "IP数量")]
    ip_amount: u32,
    #[serde(rename = "运营商")]
    isp: String,
}

fn main() -> Result<()> {
    let file = OpenOptions::new().read(true).open("output.csv")?;
    let mut reader = csv::Reader::from_reader(file);

    let mut ip_ranges = rangemap::RangeInclusiveMap::new();
    for record in reader.deserialize() {
        let Record {
            ip_start,
            ip_amount,
            isp,
        } = record?;

        let ip_bits = ip_start.to_bits();
        let end = ip_bits + ip_amount;
        ip_ranges.insert(ip_bits..=end, isp);
    }

    let records: Vec<_> = ip_ranges
        .into_iter()
        .map(|(range, isp)| {
            let bits = *range.start();
            let ip_amount = *range.end() - bits;
            let ip_start = Ipv4Addr::from_bits(bits);

            Record {
                ip_start,
                ip_amount,
                isp,
            }
        })
        .collect();

    let output_file = OpenOptions::new()
        .write(true)
        .create(true)
        .append(false)
        .open("output_merged.csv")?;
    let mut writer = csv::WriterBuilder::new()
        .delimiter(b',')
        .quote_style(csv::QuoteStyle::Never)
        .from_writer(output_file);
    for record in records {
        writer.serialize(record)?;
    }

    Ok(())
}
