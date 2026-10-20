//! Random numbers and the distributions used for query durations and arrivals.
//!
//! A tiny splitmix64 generator keeps the harness free of extra dependencies and makes
//! runs reproducible from a seed.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

/// splitmix64: fast, decent quality, good enough for load shaping.
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in (0, 1], so `ln` never sees zero.
    pub fn next_unit(&mut self) -> f64 {
        ((self.next_u64() >> 11) as f64 + 1.0) / (1u64 << 53) as f64
    }

    /// Exponentially distributed with the given mean.
    pub fn exp(&mut self, mean: f64) -> f64 {
        -mean * self.next_unit().ln()
    }
}

/// How long a simulated query holds its slot.
#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Dist {
    /// Always the same duration (`0` releases right after the grant).
    Fixed { ms: f64 },
    /// Exponential with this mean (many short queries, a few long ones).
    Exp { mean_ms: f64 },
}

impl Dist {
    pub fn sample(&self, rng: &mut Rng) -> Duration {
        let ms = match *self {
            Dist::Fixed { ms } => ms,
            Dist::Exp { mean_ms } => rng.exp(mean_ms),
        };
        Duration::from_secs_f64((ms / 1000.0).max(0.0))
    }
}

impl fmt::Display for Dist {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Dist::Fixed { ms } => write!(f, "fixed:{ms}ms"),
            Dist::Exp { mean_ms } => write!(f, "exp:{mean_ms}ms"),
        }
    }
}

impl FromStr for Dist {
    type Err = String;

    /// `0`, `25` or `fixed:25` (milliseconds), or `exp:25` (exponential, mean 25 ms).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        let (kind, value) = s.split_once(':').unwrap_or(("fixed", s));
        let ms: f64 =
            value.trim().trim_end_matches("ms").parse().map_err(|_| {
                format!("invalid duration `{s}`: expected e.g. 0, fixed:25 or exp:25")
            })?;
        if !ms.is_finite() || ms < 0.0 {
            return Err(format!("invalid duration `{s}`: must be >= 0"));
        }
        match kind.trim() {
            "fixed" => Ok(Dist::Fixed { ms }),
            "exp" if ms > 0.0 => Ok(Dist::Exp { mean_ms: ms }),
            "exp" => Err("exp: mean must be > 0".into()),
            other => Err(format!("unknown distribution `{other}` (use fixed or exp)")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_durations() {
        assert_eq!("0".parse::<Dist>().unwrap(), Dist::Fixed { ms: 0.0 });
        assert_eq!("25".parse::<Dist>().unwrap(), Dist::Fixed { ms: 25.0 });
        assert_eq!(
            "fixed:5ms".parse::<Dist>().unwrap(),
            Dist::Fixed { ms: 5.0 }
        );
        assert_eq!(
            "exp:20".parse::<Dist>().unwrap(),
            Dist::Exp { mean_ms: 20.0 }
        );
        assert!("exp:0".parse::<Dist>().is_err());
        assert!("normal:3".parse::<Dist>().is_err());
        assert!("-1".parse::<Dist>().is_err());
    }

    #[test]
    fn exponential_mean_is_close() {
        let mut rng = Rng::new(7);
        let d = Dist::Exp { mean_ms: 10.0 };
        let n = 20_000;
        let total: f64 = (0..n).map(|_| d.sample(&mut rng).as_secs_f64()).sum();
        let mean_ms = total / n as f64 * 1000.0;
        assert!((mean_ms - 10.0).abs() < 0.5, "mean {mean_ms}");
    }

    #[test]
    fn unit_is_never_zero() {
        let mut rng = Rng::new(0);
        for _ in 0..10_000 {
            let u = rng.next_unit();
            assert!(u > 0.0 && u <= 1.0);
        }
    }
}
