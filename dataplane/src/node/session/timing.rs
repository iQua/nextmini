use tokio::time::Duration;

#[cfg(test)]
const QUORUM_SOLICITATION_INTERVAL: Duration = Duration::from_millis(10);
#[cfg(not(test))]
const QUORUM_SOLICITATION_INTERVAL: Duration = Duration::from_millis(250);

#[cfg(test)]
const PEER_REPORT_TIMEOUT: Duration = Duration::from_millis(30);

#[cfg(test)]
const CONTROL_PATH_RTT_BUDGET: Duration = Duration::from_millis(15);
#[cfg(not(test))]
const CONTROL_PATH_RTT_BUDGET: Duration = Duration::from_millis(150);

#[cfg(test)]
const PASSIVE_COMPLETE_MARGIN: Duration = Duration::from_millis(15);
#[cfg(not(test))]
const PASSIVE_COMPLETE_MARGIN: Duration = Duration::from_millis(150);

#[cfg(test)]
const PASSIVE_COMPLETE_REPORT_GRACE_CAP: Duration = Duration::from_millis(200);
#[cfg(not(test))]
const PASSIVE_COMPLETE_REPORT_GRACE_CAP: Duration = Duration::from_secs(5);

pub(crate) fn quorum_solicitation_interval() -> Duration {
    QUORUM_SOLICITATION_INTERVAL
}

#[cfg(test)]
pub(crate) fn peer_report_timeout() -> Duration {
    PEER_REPORT_TIMEOUT
}

pub(crate) fn control_path_rtt_budget() -> Duration {
    CONTROL_PATH_RTT_BUDGET
}

pub(crate) fn session_finish_timeout_for(peer_report_timeout: Duration) -> Duration {
    let report_grace = peer_report_timeout.min(PASSIVE_COMPLETE_REPORT_GRACE_CAP);
    report_grace + control_path_rtt_budget() + PASSIVE_COMPLETE_MARGIN
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passive_complete_timeout_is_capped() {
        assert_eq!(
            session_finish_timeout_for(Duration::from_secs(600)),
            PASSIVE_COMPLETE_REPORT_GRACE_CAP + CONTROL_PATH_RTT_BUDGET + PASSIVE_COMPLETE_MARGIN
        );
    }
}
