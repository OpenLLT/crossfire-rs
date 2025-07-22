const SPIN_LIMIT: u16 = 6;
const DEFAULT_LIMIT: u16 = 8;

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct BackoffConfig {
    pub spin_limit: u16,
    pub limit: u16,
}

impl Default for BackoffConfig {
    #[inline(always)]
    fn default() -> Self {
        Self { spin_limit: SPIN_LIMIT, limit: DEFAULT_LIMIT }
    }
}

impl BackoffConfig {
    #[inline(always)]
    pub fn to_u32(&self) -> u32 {
        return (self.spin_limit as u32) << 16 | self.limit as u32;
    }

    #[inline(always)]
    pub fn from_u32(config: u32) -> Self {
        let limit = config as u16;
        let spin_limit = (config >> 16) as u16;
        Self { limit, spin_limit }
    }

    #[inline(always)]
    pub fn set_async_limit(&mut self, limit: u16) {
        if limit < self.limit {
            self.limit = limit;
        }
        self.spin_limit = limit;
    }
}

pub struct Backoff {
    step: u32,
    config: BackoffConfig,
}

impl Backoff {
    #[inline(always)]
    pub fn new(config: BackoffConfig) -> Self {
        Self { step: 0, config }
    }

    #[inline(always)]
    pub fn snooze(&mut self) {
        if self.step < self.config.spin_limit as u32 {
            for _ in 0..1 << self.step {
                std::hint::spin_loop();
            }
        } else {
            std::thread::yield_now();
        }
        if self.step < self.config.limit as u32 {
            self.step += 1;
        }
    }

    #[inline(always)]
    pub fn is_completed(&self) -> bool {
        self.step >= self.config.limit as u32
    }

    #[inline(always)]
    pub fn step(&self) -> usize {
        self.step as usize
    }

    #[inline(always)]
    pub fn reset(&mut self) {
        self.step = 0;
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_backoff() {
        let backoff = Backoff::new(BackoffConfig { spin_limit: 1, limit: 0 });
        assert!(backoff.is_completed())
    }
}
