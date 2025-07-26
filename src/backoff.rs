const SPIN_LIMIT: u16 = 6;
const DEFAULT_LIMIT: u16 = 6;
const MAX_LIMIT: u16 = 10;
use std::mem::transmute;

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
        let i: &u32 = unsafe { transmute(self) };
        return *i;
    }

    #[inline(always)]
    pub fn from_u32(config: u32) -> Self {
        unsafe { transmute(config) }
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
    step: u16,
    pub config: BackoffConfig,
}

impl Backoff {
    #[inline(always)]
    pub fn new(config: BackoffConfig) -> Self {
        Self { step: 0, config }
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn spin(&mut self) {
        for _ in 0..1 << self.step {
            std::hint::spin_loop();
        }
        if self.step < MAX_LIMIT {
            self.step += 1;
        }
    }

    #[inline(always)]
    pub fn snooze(&mut self) {
        if self.step < self.config.spin_limit {
            for _ in 0..1 << self.step {
                std::hint::spin_loop();
            }
        } else {
            std::thread::yield_now();
        }
        if self.step < MAX_LIMIT {
            self.step += 1;
        }
    }

    #[inline(always)]
    pub fn is_completed(&self) -> bool {
        self.step >= self.config.limit
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
        assert!(backoff.is_completed());
        println!("backoff size {}", size_of::<Backoff>());
        println!("BackoffConfig size {}", size_of::<BackoffConfig>());
        assert_eq!(size_of::<BackoffConfig>(), size_of::<u32>());
        let config = BackoffConfig { spin_limit: 6, limit: 7 };
        let config_i = config.to_u32();
        let _config = BackoffConfig::from_u32(config_i);
        assert_eq!(config.spin_limit, _config.spin_limit);
        assert_eq!(config.limit, _config.limit);

        let mut backoff = Backoff::new(BackoffConfig { spin_limit: 2, limit: 4 });
        assert_eq!(backoff.step, 0);
        backoff.spin();
        assert_eq!(backoff.step, 1);
        backoff.snooze();
        assert_eq!(backoff.step, 2);
        backoff.snooze();
        backoff.snooze();
        backoff.snooze();
        backoff.snooze();
        assert_eq!(backoff.step, 6);
        backoff.spin();
        assert_eq!(backoff.step, 7);
    }
}
