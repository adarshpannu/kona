
use std::ops;
struct COL(usize);

trait Expr {}

impl ops::Add for COL {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {x: self.x + other.x, y: self.y + other.y}
    }
}
