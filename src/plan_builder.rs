use crate::errors::{Error, Result};
use crate::match_maker::{self, Matching};
use crate::migration::Migration;

#[derive(Debug, Clone)]
pub struct Plan<'a>(Vec<Step<'a>>);

impl<'a, T> PartialEq<[T]> for Plan<'a> where Step<'a>: PartialEq<T> {
    fn eq(&self, other: &[T]) -> bool {
        self.0 == other
    }
}

impl<'a, T, const LEN: usize> PartialEq<[T; LEN]> for Plan<'a> where Step<'a>: PartialEq<T> {
    fn eq(&self, other: &[T; LEN]) -> bool {
        self.0 == other
    }
}

impl<'a, 'b: 'a> IntoIterator for &'a Plan<'b> {
    type Item = &'a Step<'b>;
    type IntoIter = std::slice::Iter<'a, Step<'b>>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Step<'a>(pub Dir, pub &'a Migration);

impl<'a> PartialEq<(Dir, &Migration)> for Step<'a> {
    fn eq(&self, other: &(Dir, &Migration)) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum Dir {
    Up,
    Down,
}

pub struct PlanBuilder<'a> {
    local_migrations: Option<&'a [Migration]>,
    db_migrations: Option<&'a [Migration]>,
    count: Option<usize>,
    strict: bool,
    ignore_divergent: bool,
    ignore_unreversable: bool,
}

pub struct PlanBuilder2<'a> {
    matches: Vec<Matching<'a>>,
    count: Option<usize>,
    strict: bool,
    ignore_divergent: bool,
    ignore_unreversable: bool,
}

impl<'a> PlanBuilder<'a> {
    pub fn new() -> Self {
        Self {
            local_migrations: None,
            db_migrations: None,
            count: None,
            strict: false,
            ignore_divergent: false,
            ignore_unreversable: false,
        }
    }

    pub fn local_migrations(mut self, m: &'a [Migration]) -> Self {
        self.local_migrations = Some(m);
        self
    }

    pub fn db_migrations(mut self, m: &'a [Migration]) -> Self {
        self.db_migrations = Some(m);
        self
    }

    pub fn count(mut self, count: Option<usize>) -> Self {
        self.count = count;
        self
    }

    pub fn set_strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    pub fn set_ignore_divergent(mut self, ignore: bool) -> Self {
        self.ignore_divergent = ignore;
        self
    }

    pub fn set_ignore_unreversable(mut self, ignore: bool) -> Self {
        self.ignore_unreversable = ignore;
        self
    }

    pub fn build(self) -> Result<PlanBuilder2<'a>> {
        if let (Some(local_migrations), Some(db_migrations)) =
            (self.local_migrations, self.db_migrations)
        {
            let mut matches = match_maker::find_matches(local_migrations, db_migrations);
            matches.sort();
            let count = self.count;
            let strict = self.strict;
            let ignore_divergent = self.ignore_divergent;
            let ignore_unreversable = self.ignore_unreversable;
            Ok(PlanBuilder2 { matches, count, strict, ignore_divergent, ignore_unreversable  })
        } else {
            Err(Error::Unknown)
        }
    }
}

impl<'a> PlanBuilder2<'a> {
    pub fn any_divergent(&self) -> bool {
        self.matches.iter().any(|m| matches!(m, Matching::Divergent(_)))
    }

    pub fn any_variant(&self) -> bool {
        self.matches.iter().any(|m| matches!(m, Matching::Variant(_, _)))
    }

    pub fn safe_to_migrate(&self) -> bool {
        !self.any_divergent() && !self.any_variant()
    }

    pub fn up(self) -> Result<Plan<'a>> {
        let mut dirty = false;
        let mut pending_found = false;
        let mut plan = Vec::new();

        for m in self.matches {
            match m {
                Matching::Pending(x) => {
                    pending_found = true;
                    if let Some(count) = self.count {
                        if count == plan.len() {
                            continue;
                        }
                    }

                    let step = Step(Dir::Up, x);
                    plan.push(step);
                }
                _ => {
                    if pending_found {
                        dirty = true;
                    }
                    continue;
                }
            }
        }

        if self.strict && dirty {
            return Err(Error::DirtyMigrations);
        }

        Ok(Plan(plan))
    }

    pub fn down(self) -> Result<Plan<'a>> {
        let mut plan = Vec::new();

        // Note: get_matches() returns the migrations in date-order.
        // We want the most recently run, so we have to reverse the order.
        for m in self.matches.iter().rev() {
            match m {
                Matching::Divergent(x) => {
                    if self.ignore_divergent {
                        continue;
                    }

                    plan.push(Step(Dir::Down, x));
                }
                Matching::Applied(_) | Matching::Variant(_, _) => {
                    if m.is_reversable() {
                        plan.push(Step(Dir::Down, m.get_best_down_migration()));
                    } else if !self.ignore_unreversable {
                        return Err(Error::UnrollbackableMigration);
                    }
                }
                _ => {}
            }

            if let Some(count) = self.count {
                if count == plan.len() {
                    break;
                }
            } else if plan.len() == 1 {
                break;
            }
        }

        Ok(Plan(plan))
    }

    pub fn fix(self) -> Result<Plan<'a>> {
        let mut bad_migration_found = false;
        let mut rollback_plan_rev = Vec::new();
        let mut rollup_plan = Vec::new();
        for m in self.matches {
            match m {
                Matching::Divergent(x) => {
                    bad_migration_found = true;
                    if m.is_reversable() {
                        rollback_plan_rev.push(Step(Dir::Down, x));
                    } else {
                        return Err(Error::UnrollbackableMigration);
                    }
                }
                Matching::Variant(_, _) => {
                    bad_migration_found = true;
                    let down = m.get_best_down_migration();
                    let up = m.get_local_migration().unwrap();
                    if m.is_reversable() {
                        rollback_plan_rev.push(Step(Dir::Down, down));
                        rollup_plan.push(Step(Dir::Up, up));
                    } else {
                        return Err(Error::UnrollbackableMigration);
                    }
                }
                Matching::Applied(x) => {
                    if bad_migration_found {
                        if m.is_reversable() {
                            rollback_plan_rev.push(Step(Dir::Down, x));
                            rollup_plan.push(Step(Dir::Up, x));
                        } else {
                            return Err(Error::UnrollbackableMigration);
                        }
                    }
                }
                Matching::Pending(x) => {
                    bad_migration_found = true;
                    rollup_plan.push(Step(Dir::Up, x));
                }
            }
        }

        let mut plan: Vec<_> = rollback_plan_rev.drain(..).rev().collect();
        plan.append(&mut rollup_plan);
        Ok(Plan(plan))
    }

    pub fn redo(self) -> Result<Plan<'a>> {
        let mut rollback_plan = Vec::new();
        let mut rollup_plan_rev = Vec::new();

        // Note: get_matches() returns the migrations in date-order.
        // We want the most recently run, so we have to reverse the order.
        for m in self.matches.iter().rev() {
            match m {
                Matching::Divergent(_) => {
                    if self.ignore_divergent {
                        continue;
                    }

                    return Err(Error::DivergentMigration);
                }
                Matching::Applied(_) | Matching::Variant(_, _) => {
                    if m.is_reversable() {
                        rollback_plan.push(Step(Dir::Down, m.get_best_down_migration()));
                        rollup_plan_rev.push(Step(Dir::Up, m.get_local_migration().unwrap()));
                    } else if !self.ignore_unreversable {
                        return Err(Error::UnrollbackableMigration);
                    }
                }
                _ => {}
            }

            if let Some(count) = self.count {
                if count == rollback_plan.len() {
                    break;
                }
            } else if rollback_plan.len() == 1 {
                break;
            }
        }

        let mut rollup_plan: Vec<_> = rollup_plan_rev.drain(..).rev().collect();
        let mut plan = rollback_plan;
        plan.append(&mut rollup_plan);
        Ok(Plan(plan))
    }

    pub fn status(self) -> Result<Vec<Matching<'a>>> {
        Ok(self.matches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // QoL impl
    mod migration {
        use movine_core::migration::Migration;

        pub fn new(name: &str) -> Migration {
            Migration {
                name: name.to_string(),
                up_sql: None,
                down_sql: Some("test".to_owned()),
                hash: None,
            }
        }

        pub fn new_with_hash(name: &str, hash: &str) -> Migration {
            Migration {
                name: name.to_string(),
                up_sql: None,
                down_sql: None,
                hash: Some(hash.to_string()),
            }
        }
    }

    #[test]
    /// Up should run pending migrations in-order.
    fn test_up_1() {
        let local = [migration::new(&"test_1"), migration::new(&"test_2")];
        let db = [];
        let plan = PlanBuilder::new()
            .local_migrations(&local)
            .db_migrations(&db)
            .build()
            .unwrap()
            .up()
            .unwrap();
        assert_eq!(plan, [(Dir::Up, &local[0]), (Dir::Up, &local[1])])
    }

    #[test]
    /// Up should run pending migrations even if divergent migrations exist.
    fn test_up_2() {
        let local = [migration::new(&"test"), migration::new(&"test_2")];
        let db = [migration::new(&"test"), migration::new(&"test_3")];
        let plan = PlanBuilder::new()
            .local_migrations(&local)
            .db_migrations(&db)
            .build()
            .unwrap()
            .up()
            .unwrap();
        assert_eq!(plan, [(Dir::Up, &local[1])])
    }

    #[test]
    /// Up should error with --strict if migrations are out-of-order.
    fn test_up_3() {
        let local = [migration::new(&"test"), migration::new(&"test_2")];
        let db = [migration::new(&"test"), migration::new(&"test_3")];
        let plan = PlanBuilder::new()
            .local_migrations(&local)
            .db_migrations(&db)
            .set_strict(true)
            .build()
            .unwrap()
            .up();
        assert!(plan.is_err());
        let is_correct_error = matches!(plan.err().unwrap(), Error::DirtyMigrations);
        assert!(is_correct_error);
    }

    #[test]
    /// Down should rollback the most recent migration (divergent included by default)
    fn test_down_1() {
        let local = [migration::new(&"test"), migration::new(&"test_2")];
        let db = [migration::new(&"test"), migration::new(&"test_3")];
        let plan = PlanBuilder::new()
            .local_migrations(&local)
            .db_migrations(&db)
            .build()
            .unwrap()
            .down()
            .unwrap();
        assert_eq!(plan, [(Dir::Down, &db[1])])
    }

    #[test]
    /// Down should rollback the most recent migration (ignoring divergent)
    fn test_down_2() {
        let local = [migration::new(&"test"), migration::new(&"test_2")];
        let db = [migration::new(&"test"), migration::new(&"test_3")];
        let plan = PlanBuilder::new()
            .local_migrations(&local)
            .db_migrations(&db)
            .set_ignore_divergent(true)
            .build()
            .unwrap()
            .down()
            .unwrap();
        assert_eq!(plan, [(Dir::Down, &local[0])])
    }

    #[test]
    /// Fix should rollback all variant and divergent migrations, and then run pending migrations.
    fn test_fix_1() {
        let local = [
            migration::new(&"test_0"),
            migration::new(&"test_1"),
            migration::new(&"test_2"),
        ];
        let db = [
            migration::new(&"test_0"),
            migration::new_with_hash(&"test_1", &"hash"),
            migration::new_with_hash(&"test_2", &"hash"),
            migration::new(&"test_3"),
        ];
        let plan = PlanBuilder::new()
            .local_migrations(&local)
            .db_migrations(&db)
            .build()
            .unwrap()
            .fix()
            .unwrap();
        assert_eq!(
            plan,
            [
                (Dir::Down, &db[3]),
                (Dir::Down, &local[2]),
                (Dir::Down, &local[1]),
                (Dir::Up, &local[1]),
                (Dir::Up, &local[2]),
            ]
        )
    }

    #[test]
    /// Fix should rollback applied migrations if they are ahead of variant migrations.
    fn test_fix_2() {
        let local = [
            migration::new(&"test"),
            migration::new(&"test_1"),
            migration::new(&"test_2"),
        ];
        let db = [
            migration::new(&"test"),
            migration::new_with_hash(&"test_1", &"hash"),
            migration::new(&"test_2"),
        ];
        let plan = PlanBuilder::new()
            .local_migrations(&local)
            .db_migrations(&db)
            .build()
            .unwrap()
            .fix()
            .unwrap();
        assert_eq!(
            plan,
            [
                (Dir::Down, &local[2]),
                (Dir::Down, &local[1]),
                (Dir::Up, &local[1]),
                (Dir::Up, &local[2]),
            ]
        )
    }

    #[test]
    /// Fix should rollback everything to a fully applied state and then roll back up, regardless
    /// of applied/variant/diverget migration orders.
    fn test_fix_3() {
        let local = [
            migration::new(&"test_0"),
            migration::new(&"test_1"),
            migration::new(&"test_2"),
            migration::new(&"test_3"),
            migration::new(&"test_4"),
        ];
        let db = [
            migration::new(&"test_0"),
            migration::new_with_hash(&"test_1", &"hash"),
            migration::new(&"test_2"),
            migration::new(&"test_3b"),
            migration::new(&"test_4"),
        ];
        let actual = PlanBuilder::new()
            .local_migrations(&local)
            .db_migrations(&db)
            .build()
            .unwrap()
            .fix()
            .unwrap();
        let expected = [
            (Dir::Down, &local[4]),
            (Dir::Down, &db[3]),
            (Dir::Down, &local[2]),
            (Dir::Down, &local[1]),
            (Dir::Up, &local[1]),
            (Dir::Up, &local[2]),
            (Dir::Up, &local[3]),
            (Dir::Up, &local[4]),
        ];
        assert_eq!(actual, expected)
    }

    #[test]
    /// Fix should run pending migrations without problems.
    fn test_fix_4() {
        let local = [migration::new(&"test_0"), migration::new(&"test_1")];
        let db = [migration::new(&"test_0")];
        let actual = PlanBuilder::new()
            .local_migrations(&local)
            .db_migrations(&db)
            .build()
            .unwrap()
            .fix()
            .unwrap();
        let expected = [(Dir::Up, &local[1])];
        assert_eq!(actual, expected)
    }

    #[test]
    /// Redo should fail if there is a divergent migration (and we are not ignoring them)
    fn test_redo_1() {
        let local = [migration::new(&"test"), migration::new(&"test_2")];
        let db = [
            migration::new(&"test"),
            migration::new_with_hash(&"test_2", &"hash_1"),
            migration::new(&"test_3"),
        ];
        let plan = PlanBuilder::new()
            .local_migrations(&local)
            .db_migrations(&db)
            .count(Some(2))
            .build()
            .unwrap()
            .redo();
        assert!(plan.is_err());
        let is_correct_err = matches!(plan.err().unwrap(), Error::DivergentMigration);
        assert!(is_correct_err);
    }

    #[test]
    /// Redo should properly ignore divergent migrations
    fn test_redo_2() {
        let local = [
            migration::new(&"test_0"),
            migration::new(&"test_1"),
            migration::new(&"test_2"),
        ];
        let db = [
            migration::new(&"test_0"),
            migration::new(&"test_1"),
            migration::new(&"test_2"),
            migration::new(&"test_3"),
        ];
        let plan = PlanBuilder::new()
            .local_migrations(&local)
            .db_migrations(&db)
            .count(Some(2))
            .set_ignore_divergent(true)
            .build()
            .unwrap()
            .redo()
            .unwrap();
        assert_eq!(
            plan,
            [
                (Dir::Down, &local[2]),
                (Dir::Down, &local[1]),
                (Dir::Up, &local[1]),
                (Dir::Up, &local[2]),
            ]
        )
    }

    #[test]
    /// Redo should not care about variant migrations further than what we are redo'ing
    fn test_redo_3() {
        let local = [
            migration::new(&"test_0"),
            migration::new(&"test_1"),
            migration::new(&"test_2"),
        ];
        let db = [
            migration::new(&"test_0"),
            migration::new_with_hash(&"test_1", &"hash_1"),
            migration::new(&"test_2"),
        ];
        let plan = PlanBuilder::new()
            .local_migrations(&local)
            .db_migrations(&db)
            .count(Some(1))
            .build()
            .unwrap()
            .redo()
            .unwrap();
        assert_eq!(plan, [(Dir::Down, &local[2]), (Dir::Up, &local[2]),])
    }

    #[test]
    /// Redo should properly rollback variant migrations
    fn test_redo_4() {
        let local = [
            migration::new(&"test_0"),
            migration::new(&"test_1"),
            migration::new(&"test_2"),
        ];
        let db = [
            migration::new(&"test_0"),
            migration::new_with_hash(&"test_1", &"hash_1"),
            migration::new(&"test_2"),
        ];
        let plan = PlanBuilder::new()
            .local_migrations(&local)
            .db_migrations(&db)
            .count(Some(2))
            .build()
            .unwrap()
            .redo()
            .unwrap();
        assert_eq!(
            plan,
            [
                (Dir::Down, &local[2]),
                (Dir::Down, &local[1]),
                (Dir::Up, &local[1]),
                (Dir::Up, &local[2]),
            ]
        )
    }
}
