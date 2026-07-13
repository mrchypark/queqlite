use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize)]
pub struct RatesFile {
    pub as_of: String,
    pub currency: String,
    pub providers: Vec<ProviderRate>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ProviderRate {
    pub id: String,
    pub provider: String,
    pub storage_class: String,
    pub region: String,
    pub storage_unit: String,
    pub storage_input_gb_month_multiplier: f64,
    pub storage_usd_per_unit_month: f64,
    pub put_list_usd_per_1000: f64,
    pub get_usd_per_1000: f64,
    pub delete_usd_per_1000: f64,
    pub default_egress_usd_per_gb: f64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct CostInput {
    pub retained_gb_month: f64,
    pub put_count: u64,
    pub list_count: u64,
    pub get_count: u64,
    pub delete_count: u64,
    pub egress_gb: f64,
    pub egress_usd_per_gb: Option<f64>,
    pub rustfs_storage_usd_per_gb_month: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct CostOutput {
    pub as_of: String,
    pub currency: String,
    pub provider: String,
    pub storage_class: String,
    pub region: String,
    pub items: Vec<CostItem>,
    pub total_monthly_usd: f64,
}

#[derive(Debug, Serialize)]
pub struct CostItem {
    pub name: &'static str,
    pub quantity: f64,
    pub unit: String,
    pub rate_usd: f64,
    pub rate_unit: String,
    pub monthly_usd: f64,
}

pub fn parse_rates(source: &str) -> Result<RatesFile, String> {
    serde_json::from_str(source).map_err(|error| format!("parse rates JSON: {error}"))
}

pub fn calculate(
    rates: &RatesFile,
    provider_id: &str,
    input: CostInput,
) -> Result<CostOutput, String> {
    validate_input(input)?;
    let provider = rates
        .providers
        .iter()
        .find(|provider| provider.id == provider_id)
        .ok_or_else(|| format!("unknown provider {provider_id:?}"))?;
    validate_provider(provider)?;
    let storage_rate = match input.rustfs_storage_usd_per_gb_month {
        Some(rate) if provider.id == "rustfs-local" => rate,
        Some(_) => {
            return Err("--rustfs-storage-usd-per-gb-month only applies to rustfs-local".into())
        }
        None if provider.id == "rustfs-local" => {
            return Err("--rustfs-storage-usd-per-gb-month is required for rustfs-local".into())
        }
        None => provider.storage_usd_per_unit_month,
    };
    if storage_rate < 0.0 || !storage_rate.is_finite() {
        return Err("storage rate must be a finite non-negative number".into());
    }
    let egress_rate = input
        .egress_usd_per_gb
        .unwrap_or(provider.default_egress_usd_per_gb);
    if egress_rate < 0.0 || !egress_rate.is_finite() {
        return Err("egress rate must be a finite non-negative number".into());
    }

    let storage_quantity = input.retained_gb_month * provider.storage_input_gb_month_multiplier;
    let items = vec![
        item(
            "storage",
            storage_quantity,
            format!("{}-month", provider.storage_unit),
            storage_rate,
            format!("USD per {}-month", provider.storage_unit),
        ),
        item(
            "put",
            input.put_count as f64,
            "requests".into(),
            provider.put_list_usd_per_1000,
            "USD per 1,000 requests".into(),
        ),
        item(
            "list",
            input.list_count as f64,
            "requests".into(),
            provider.put_list_usd_per_1000,
            "USD per 1,000 requests".into(),
        ),
        item(
            "get",
            input.get_count as f64,
            "requests".into(),
            provider.get_usd_per_1000,
            "USD per 1,000 requests".into(),
        ),
        item(
            "delete",
            input.delete_count as f64,
            "requests".into(),
            provider.delete_usd_per_1000,
            "USD per 1,000 requests".into(),
        ),
        item(
            "egress",
            input.egress_gb,
            "GB".into(),
            egress_rate,
            "USD per GB".into(),
        ),
    ];
    let total_monthly_usd = items.iter().map(|item| item.monthly_usd).sum();
    Ok(CostOutput {
        as_of: rates.as_of.clone(),
        currency: rates.currency.clone(),
        provider: provider.provider.clone(),
        storage_class: provider.storage_class.clone(),
        region: provider.region.clone(),
        items,
        total_monthly_usd,
    })
}

fn validate_input(input: CostInput) -> Result<(), String> {
    for (name, value) in [
        ("retained GB-month", input.retained_gb_month),
        ("egress GB", input.egress_gb),
    ] {
        if value < 0.0 || !value.is_finite() {
            return Err(format!("{name} must be a finite non-negative number"));
        }
    }
    Ok(())
}

fn validate_provider(provider: &ProviderRate) -> Result<(), String> {
    for (name, value) in [
        (
            "storage input multiplier",
            provider.storage_input_gb_month_multiplier,
        ),
        ("storage rate", provider.storage_usd_per_unit_month),
        ("PUT/LIST rate", provider.put_list_usd_per_1000),
        ("GET rate", provider.get_usd_per_1000),
        ("DELETE rate", provider.delete_usd_per_1000),
        ("default egress rate", provider.default_egress_usd_per_gb),
    ] {
        if value < 0.0 || !value.is_finite() {
            return Err(format!(
                "provider {name} must be a finite non-negative number"
            ));
        }
    }
    Ok(())
}

fn item(
    name: &'static str,
    quantity: f64,
    unit: String,
    rate_usd: f64,
    rate_unit: String,
) -> CostItem {
    let divisor = if rate_unit == "USD per 1,000 requests" {
        1_000.0
    } else {
        1.0
    };
    CostItem {
        name,
        quantity,
        unit,
        rate_usd,
        rate_unit,
        monthly_usd: quantity / divisor * rate_usd,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RATES: &str = include_str!("../rates-2026-07-12.json");

    #[test]
    fn aws_formula_itemizes_storage_calls_and_explicit_egress() {
        let rates = parse_rates(RATES).unwrap();
        let output = calculate(
            &rates,
            "aws-s3-standard-us-east-1",
            CostInput {
                retained_gb_month: 100.0,
                put_count: 2_000,
                list_count: 1_000,
                get_count: 1_000,
                delete_count: 1_000,
                egress_gb: 10.0,
                egress_usd_per_gb: Some(0.09),
                rustfs_storage_usd_per_gb_month: None,
            },
        )
        .unwrap();

        assert_eq!(output.items[0].unit, "GiB-month");
        assert!((output.total_monthly_usd - 3.0574419216156).abs() < 1e-12);
        assert_eq!(output.items[4].monthly_usd, 0.0);
    }

    #[test]
    fn azure_converts_the_gb_month_input_to_its_gib_month_rate_unit() {
        let rates = parse_rates(RATES).unwrap();
        let output = calculate(
            &rates,
            "azure-blob-hot-lrs-eastus2",
            CostInput {
                retained_gb_month: 100.0,
                ..CostInput::default()
            },
        )
        .unwrap();

        assert_eq!(output.items[0].unit, "GiB-month");
        assert!((output.items[0].monthly_usd - 1.7136335372924805).abs() < 1e-12);
    }

    #[test]
    fn gcs_converts_the_gb_month_input_to_its_gib_month_rate_unit() {
        let rates = parse_rates(RATES).unwrap();
        let output = calculate(
            &rates,
            "gcs-standard-us-central1",
            CostInput {
                retained_gb_month: 100.0,
                ..CostInput::default()
            },
        )
        .unwrap();

        assert_eq!(output.items[0].unit, "GiB-month");
        assert!((output.items[0].monthly_usd - 1.862645149230957).abs() < 1e-12);
    }

    #[test]
    fn rustfs_uses_the_supplied_local_storage_rate_and_has_no_call_fees() {
        let rates = parse_rates(RATES).unwrap();
        let output = calculate(
            &rates,
            "rustfs-local",
            CostInput {
                retained_gb_month: 100.0,
                put_count: 99,
                list_count: 99,
                get_count: 99,
                delete_count: 99,
                rustfs_storage_usd_per_gb_month: Some(0.01),
                ..CostInput::default()
            },
        )
        .unwrap();

        assert_eq!(output.total_monthly_usd, 1.0);
    }

    #[test]
    fn rustfs_requires_an_explicit_local_storage_rate() {
        let rates = parse_rates(RATES).unwrap();
        let error = calculate(
            &rates,
            "rustfs-local",
            CostInput {
                retained_gb_month: 100.0,
                ..CostInput::default()
            },
        )
        .unwrap_err();

        assert!(error.contains("--rustfs-storage-usd-per-gb-month"));
    }

    #[test]
    fn provider_rates_must_be_finite_and_non_negative() {
        for field in 0..6 {
            let mut rates = parse_rates(RATES).unwrap();
            let provider = &mut rates.providers[0];
            match field {
                0 => provider.storage_input_gb_month_multiplier = -1.0,
                1 => provider.storage_usd_per_unit_month = -1.0,
                2 => provider.put_list_usd_per_1000 = -1.0,
                3 => provider.get_usd_per_1000 = -1.0,
                4 => provider.delete_usd_per_1000 = -1.0,
                5 => provider.default_egress_usd_per_gb = f64::NAN,
                _ => unreachable!(),
            }
            let provider_id = provider.id.clone();

            assert!(calculate(&rates, &provider_id, CostInput::default()).is_err());
        }
    }
}
