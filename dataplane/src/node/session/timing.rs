use tokio::time::Duration;

#[cfg(test)]
const QUORUM_SOLICITATION_INTERVAL: Duration = Duration::from_millis(10);
#[cfg(not(test))]
const QUORUM_SOLICITATION_INTERVAL: Duration = Duration::from_millis(250);

#[cfg(test)]
const PEER_REPORT_TIMEOUT: Duration = Duration::from_millis(30);
#[cfg(not(test))]
#[allow(dead_code)]
const PEER_REPORT_TIMEOUT: Duration = Duration::from_millis(1500);

#[cfg(test)]
const CONTROL_PATH_RTT_BUDGET: Duration = Duration::from_millis(15);
#[cfg(not(test))]
const CONTROL_PATH_RTT_BUDGET: Duration = Duration::from_millis(150);

#[cfg(test)]
const PASSIVE_COMPLETE_MARGIN: Duration = Duration::from_millis(15);
#[cfg(not(test))]
const PASSIVE_COMPLETE_MARGIN: Duration = Duration::from_millis(150);

pub(crate) fn quorum_solicitation_interval() -> Duration {
    QUORUM_SOLICITATION_INTERVAL
}

#[allow(dead_code)]
pub(crate) fn peer_report_timeout() -> Duration {
    PEER_REPORT_TIMEOUT
}

pub(crate) fn control_path_rtt_budget() -> Duration {
    CONTROL_PATH_RTT_BUDGET
}

pub(crate) fn session_finish_timeout_for(peer_report_timeout: Duration) -> Duration {
    peer_report_timeout + control_path_rtt_budget() + PASSIVE_COMPLETE_MARGIN
}

#[allow(dead_code)]
pub(crate) fn session_finish_timeout() -> Duration {
    session_finish_timeout_for(peer_report_timeout())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passive_complete_timeout_exceeds_sender_timeout_and_control_budget() {
        assert!(session_finish_timeout() > peer_report_timeout() + control_path_rtt_budget());
    }
}
