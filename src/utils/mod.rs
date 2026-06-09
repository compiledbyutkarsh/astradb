use std::sync::Arc;

use parking_lot::RwLock;

#[derive(Default)]
pub struct MetricsData {
    pub total_connections: usize,

    pub active_connections: usize,

    pub total_commands: usize,

    pub total_reads: usize,

    pub total_writes: usize,
}

#[derive(Clone)]
pub struct Metrics {
    inner:
        Arc<RwLock<MetricsData>>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(
                RwLock::new(
                    MetricsData::default(),
                ),
            ),
        }
    }

    pub fn connection_opened(
        &self,
    ) {
        let mut metrics =
            self.inner.write();

        metrics.total_connections += 1;

        metrics.active_connections += 1;
    }

    pub fn connection_closed(
        &self,
    ) {
        let mut metrics =
            self.inner.write();

        if metrics.active_connections > 0 {
            metrics.active_connections -= 1;
        }
    }

    pub fn command_executed(
        &self,
    ) {
        let mut metrics =
            self.inner.write();

        metrics.total_commands += 1;
    }

    pub fn read_operation(
        &self,
    ) {
        let mut metrics =
            self.inner.write();

        metrics.total_reads += 1;
    }

    pub fn write_operation(
        &self,
    ) {
        let mut metrics =
            self.inner.write();

        metrics.total_writes += 1;
    }

    pub fn snapshot(
        &self,
    ) -> MetricsData {
        let metrics =
            self.inner.read();

        MetricsData {
            total_connections:
                metrics.total_connections,

            active_connections:
                metrics.active_connections,

            total_commands:
                metrics.total_commands,

            total_reads:
                metrics.total_reads,

            total_writes:
                metrics.total_writes,
        }
    }
}