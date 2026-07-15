use std::{env, fs, path::PathBuf, process};

use rhiza_bench::cost::{calculate, parse_rates, CostInput};

const DEFAULT_RATES: &str = include_str!("../../rates-2026-07-12.json");

fn main() {
    if let Err(error) = run() {
        eprintln!("cost calculator error: {error}");
        print_usage();
        process::exit(2);
    }
}

fn run() -> Result<(), String> {
    let mut provider = None;
    let mut retained_gb_month = None;
    let mut input = CostInput::default();
    let mut rates_path = None;
    let mut output = None;
    let args: Vec<String> = env::args().skip(1).collect();
    let mut index = 0;
    while index < args.len() {
        let flag = &args[index];
        let value = || -> Result<&str, String> {
            args.get(index + 1)
                .map(String::as_str)
                .ok_or_else(|| format!("{flag} requires a value"))
        };
        match flag.as_str() {
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            "--provider" => {
                provider = Some(value()?.to_owned());
                index += 1;
            }
            "--retained-gb-month" => {
                retained_gb_month = Some(parse_number(value()?, flag)?);
                index += 1;
            }
            "--put-count" => {
                input.put_count = parse_count(value()?, flag)?;
                index += 1;
            }
            "--list-count" => {
                input.list_count = parse_count(value()?, flag)?;
                index += 1;
            }
            "--get-count" => {
                input.get_count = parse_count(value()?, flag)?;
                index += 1;
            }
            "--delete-count" => {
                input.delete_count = parse_count(value()?, flag)?;
                index += 1;
            }
            "--egress-gb" => {
                input.egress_gb = parse_number(value()?, flag)?;
                index += 1;
            }
            "--egress-usd-per-gb" => {
                input.egress_usd_per_gb = Some(parse_number(value()?, flag)?);
                index += 1;
            }
            "--rustfs-storage-usd-per-gb-month" => {
                input.rustfs_storage_usd_per_gb_month = Some(parse_number(value()?, flag)?);
                index += 1;
            }
            "--rates" => {
                rates_path = Some(PathBuf::from(value()?));
                index += 1;
            }
            "--output" => {
                output = Some(PathBuf::from(value()?));
                index += 1;
            }
            _ => return Err(format!("unknown option {flag:?}")),
        }
        index += 1;
    }

    input.retained_gb_month =
        retained_gb_month.ok_or_else(|| "--retained-gb-month is required".to_owned())?;
    let provider = provider.ok_or_else(|| "--provider is required".to_owned())?;
    let source = match rates_path {
        Some(path) => {
            fs::read_to_string(path).map_err(|error| format!("read rates JSON: {error}"))?
        }
        None => DEFAULT_RATES.to_owned(),
    };
    let rates = parse_rates(&source)?;
    let result = serde_json::to_string_pretty(&calculate(&rates, &provider, input)?)
        .map_err(|error| format!("encode JSON: {error}"))?;
    if let Some(path) = output {
        fs::write(path, result).map_err(|error| format!("write output: {error}"))?;
    } else {
        println!("{result}");
    }
    Ok(())
}

fn parse_number(value: &str, flag: &str) -> Result<f64, String> {
    let value = value
        .parse::<f64>()
        .map_err(|_| format!("{flag} must be a number"))?;
    if !value.is_finite() || value < 0.0 {
        return Err(format!("{flag} must be a finite non-negative number"));
    }
    Ok(value)
}

fn parse_count(value: &str, flag: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|_| format!("{flag} must be a non-negative integer"))
}

fn print_usage() {
    eprintln!(
        "Usage: rhiza-cost --provider ID --retained-gb-month GB_MONTH [options]\n\
         Options: --put-count N --list-count N --get-count N --delete-count N\n\
                  --egress-gb N --egress-usd-per-gb N\n\
                  --rustfs-storage-usd-per-gb-month N --rates FILE --output FILE"
    );
}
