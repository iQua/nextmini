//! Representative public-cloud egress-price magnitudes for policy comparisons.
//!
//! These deterministic matrices are scenario inputs, not provider measurements or billing
//! calculators. Rates are normalized to nano-USD per decimal GB. The public pages behind the
//! representative magnitudes are pinned here so reports can state exactly what was modeled:
//!
//! * AWS says inter-region transfer is charged on the source side and publishes region-specific
//!   rates; the common public magnitude is $0.02/GB, with $0.01/GB between us-east-1/us-east-2.
//!   <https://aws.amazon.com/ec2/pricing/on-demand/>
//! * Google Cloud's inter-region table publishes $0.02/GB within North America or Europe,
//!   $0.05/GB North America-Europe, and $0.08/GB for the modeled Asia pairs.
//!   <https://cloud.google.com/vpc/pricing>
//! * DigitalOcean publishes $0.01/GiB for outbound transfer beyond the included pool and says the
//!   rate has no regional variation. We use the same 0.01 numerical magnitude per decimal GB to
//!   keep one explicit unit across arms; this is representative, not a bill estimate.
//!   <https://docs.digitalocean.com/platform/billing/bandwidth/>

use super::{CloudProfileKind, CloudScenario, CloudcastEgressPrices, CloudcastPolicyError};

const USD_001: u64 = 10_000_000;
const USD_002: u64 = 20_000_000;
const USD_005: u64 = 50_000_000;
const USD_008: u64 = 80_000_000;

pub fn representative_egress_prices(
    scenario: &CloudScenario,
) -> Result<CloudcastEgressPrices, CloudcastPolicyError> {
    let mut matrix = vec![vec![0_u64; scenario.regions.len()]; scenario.regions.len()];
    for (from, row) in matrix.iter_mut().enumerate() {
        for (to, rate) in row.iter_mut().enumerate() {
            if from == to {
                continue;
            }
            *rate = match scenario.profile {
                CloudProfileKind::AwsLike => aws_rate(scenario, from, to),
                CloudProfileKind::GcpLike => gcp_rate(scenario, from, to),
                CloudProfileKind::DigitaloceanLike => USD_001,
            };
        }
    }
    CloudcastEgressPrices::new(scenario, matrix)
}

fn aws_rate(scenario: &CloudScenario, from: usize, to: usize) -> u64 {
    let pair = [
        scenario.regions[from].id.as_str(),
        scenario.regions[to].id.as_str(),
    ];
    if pair
        .iter()
        .all(|region| matches!(*region, "us-east-1" | "us-east-2"))
    {
        USD_001
    } else {
        USD_002
    }
}

fn gcp_rate(scenario: &CloudScenario, from: usize, to: usize) -> u64 {
    let from_area = area(&scenario.regions[from].hub);
    let to_area = area(&scenario.regions[to].hub);
    match (from_area, to_area) {
        (Area::NorthAmerica, Area::NorthAmerica) | (Area::Europe, Area::Europe) => USD_002,
        (Area::NorthAmerica, Area::Europe) | (Area::Europe, Area::NorthAmerica) => USD_005,
        (_, Area::Asia) | (Area::Asia, _) => USD_008,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Area {
    NorthAmerica,
    Europe,
    Asia,
}

fn area(hub: &str) -> Area {
    match hub {
        "na-east" | "na-west" => Area::NorthAmerica,
        "eu" => Area::Europe,
        "ap" => Area::Asia,
        _ => unreachable!("built-in cloud profile uses a known transit hub"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn representative_tables_pin_public_magnitudes_and_zero_diagonals() {
        let aws = CloudScenario::built_in(CloudProfileKind::AwsLike, 0).expect("AWS-like");
        let aws_prices = representative_egress_prices(&aws).expect("AWS prices");
        assert_eq!(aws_prices.rate(0, 0), Some(0));
        assert_eq!(aws_prices.rate(0, 1), Some(USD_001));
        assert_eq!(aws_prices.rate(0, 3), Some(USD_002));

        let gcp = CloudScenario::built_in(CloudProfileKind::GcpLike, 0).expect("GCP-like");
        let gcp_prices = representative_egress_prices(&gcp).expect("GCP prices");
        assert_eq!(gcp_prices.rate(0, 1), Some(USD_002));
        assert_eq!(gcp_prices.rate(0, 3), Some(USD_005));
        assert_eq!(gcp_prices.rate(0, 5), Some(USD_008));

        let digitalocean =
            CloudScenario::built_in(CloudProfileKind::DigitaloceanLike, 0).expect("DO-like");
        let do_prices = representative_egress_prices(&digitalocean).expect("DO prices");
        assert_eq!(do_prices.rate(0, 1), Some(USD_001));
        assert_eq!(do_prices.rate(1, 5), Some(USD_001));
    }
}
