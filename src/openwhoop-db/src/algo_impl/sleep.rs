use chrono::{NaiveDateTime, TimeDelta};
use openwhoop_algos::SleepCycle;
use openwhoop_entities::sleep_cycles;
use sea_orm::{ColumnTrait, Condition, EntityTrait, QueryFilter, QueryOrder};

use crate::{DailyStats, DailyStatsAverage, DatabaseHandler};

impl DatabaseHandler {
    pub async fn get_sleep_cycles(
        &self,
        start: Option<NaiveDateTime>,
    ) -> anyhow::Result<Vec<SleepCycle>> {
        let filter = Condition::all().add_option(start.map(|s| sleep_cycles::Column::Start.gte(s)));

        Ok(sleep_cycles::Entity::find()
            .order_by_asc(sleep_cycles::Column::Start)
            .filter(filter)
            .all(&self.db)
            .await?
            .into_iter()
            .map(map_sleep_cycle)
            .collect())
    }

    pub async fn get_latest_daily_stats(&self) -> anyhow::Result<Option<DailyStats>> {
        let Some(sleep) = self.get_latest_sleep().await? else {
            return Ok(None);
        };

        self.get_daily_stats_for_sleep(sleep).await.map(Some)
    }

    pub async fn get_last_7_day_daily_stats_average(&self) -> anyhow::Result<DailyStatsAverage> {
        let Some(latest_sleep) = self.get_latest_sleep().await? else {
            return Ok(DailyStatsAverage::MissingDays(7));
        };

        let window_start = latest_sleep.id - TimeDelta::days(6);
        let sleeps = sleep_cycles::Entity::find()
            .filter(sleep_cycles::Column::SleepId.gte(window_start))
            .filter(sleep_cycles::Column::SleepId.lte(latest_sleep.id))
            .order_by_asc(sleep_cycles::Column::SleepId)
            .all(&self.db)
            .await?;

        let missing_days = 7usize.saturating_sub(sleeps.len());
        if missing_days > 0 {
            return Ok(DailyStatsAverage::MissingDays(
                u8::try_from(missing_days).unwrap_or(u8::MAX),
            ));
        }

        let expected_dates = (0..7).map(|offset| window_start + TimeDelta::days(offset));
        let actual_dates = sleeps.iter().map(|sleep| sleep.sleep_id);
        let contiguous_days = expected_dates
            .zip(actual_dates)
            .all(|(expected, actual)| expected == actual);
        if !contiguous_days {
            let distinct_dates = sleeps
                .iter()
                .filter(|sleep| sleep.sleep_id >= window_start && sleep.sleep_id <= latest_sleep.id)
                .count();
            let missing = 7usize.saturating_sub(distinct_dates);
            return Ok(DailyStatsAverage::MissingDays(
                u8::try_from(missing).unwrap_or(u8::MAX),
            ));
        }

        let mut hrv_sum = 0.0;
        let mut rhr_sum = 0.0;

        for sleep in sleeps.into_iter().map(map_sleep_cycle) {
            let stats = self.get_daily_stats_for_sleep(sleep).await?;
            if let Some(hrv) = stats.hrv {
                hrv_sum += hrv;
            }
            if let Some(rhr) = stats.rhr {
                rhr_sum += rhr;
            }
        }

        Ok(DailyStatsAverage::Average(DailyStats {
            hrv: Some(hrv_sum / 7.0),
            rhr: Some(rhr_sum / 7.0),
        }))
    }

    async fn get_daily_stats_for_sleep(&self, sleep: SleepCycle) -> anyhow::Result<DailyStats> {
        Ok(DailyStats {
            hrv: Some(f64::from(sleep.avg_hrv)),
            rhr: Some(f64::from(sleep.avg_bpm)),
        })
    }
}

fn map_sleep_cycle(value: sleep_cycles::Model) -> SleepCycle {
    SleepCycle {
        id: value.sleep_id,
        start: value.start,
        end: value.end,
        min_bpm: value.min_bpm.try_into().unwrap(),
        max_bpm: value.max_bpm.try_into().unwrap(),
        avg_bpm: value.avg_bpm.try_into().unwrap(),
        min_hrv: value.min_hrv.try_into().unwrap(),
        max_hrv: value.max_hrv.try_into().unwrap(),
        avg_hrv: value.avg_hrv.try_into().unwrap(),
        score: value
            .score
            .unwrap_or(SleepCycle::sleep_score(value.start, value.end)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn map_sleep_cycle_with_score() {
        let model = sleep_cycles::Model {
            id: uuid::Uuid::new_v4(),
            sleep_id: NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
            start: NaiveDate::from_ymd_opt(2025, 1, 1)
                .unwrap()
                .and_hms_opt(22, 0, 0)
                .unwrap(),
            end: NaiveDate::from_ymd_opt(2025, 1, 2)
                .unwrap()
                .and_hms_opt(6, 0, 0)
                .unwrap(),
            min_bpm: 50,
            max_bpm: 70,
            avg_bpm: 60,
            min_hrv: 30,
            max_hrv: 80,
            avg_hrv: 55,
            score: Some(95.0),
            synced: false,
        };

        let cycle = map_sleep_cycle(model);
        assert_eq!(cycle.min_bpm, 50);
        assert_eq!(cycle.avg_hrv, 55);
        assert_eq!(cycle.score, 95.0);
    }

    #[test]
    fn map_sleep_cycle_without_score_uses_calculated() {
        let model = sleep_cycles::Model {
            id: uuid::Uuid::new_v4(),
            sleep_id: NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
            start: NaiveDate::from_ymd_opt(2025, 1, 1)
                .unwrap()
                .and_hms_opt(22, 0, 0)
                .unwrap(),
            end: NaiveDate::from_ymd_opt(2025, 1, 2)
                .unwrap()
                .and_hms_opt(6, 0, 0)
                .unwrap(),
            min_bpm: 50,
            max_bpm: 70,
            avg_bpm: 60,
            min_hrv: 30,
            max_hrv: 80,
            avg_hrv: 55,
            score: None, // No score stored
            synced: false,
        };

        let cycle = map_sleep_cycle(model);
        // 8 hours / 8 hours = 1.0 -> 100.0
        assert_eq!(cycle.score, 100.0);
    }

    #[tokio::test]
    async fn get_sleep_cycles_empty() {
        let db = DatabaseHandler::new("sqlite::memory:").await;
        let cycles = db.get_sleep_cycles(None).await.unwrap();
        assert!(cycles.is_empty());
    }

    #[tokio::test]
    async fn get_sleep_cycles_returns_inserted() {
        let db = DatabaseHandler::new("sqlite::memory:").await;

        let start = NaiveDate::from_ymd_opt(2025, 1, 1)
            .unwrap()
            .and_hms_opt(22, 0, 0)
            .unwrap();
        let end = NaiveDate::from_ymd_opt(2025, 1, 2)
            .unwrap()
            .and_hms_opt(6, 0, 0)
            .unwrap();

        db.create_sleep(SleepCycle {
            id: end.date(),
            start,
            end,
            min_bpm: 50,
            max_bpm: 70,
            avg_bpm: 60,
            min_hrv: 30,
            max_hrv: 80,
            avg_hrv: 55,
            score: 100.0,
        })
        .await
        .unwrap();

        let cycles = db.get_sleep_cycles(None).await.unwrap();
        assert_eq!(cycles.len(), 1);
        assert_eq!(cycles[0].min_bpm, 50);
    }

    #[tokio::test]
    async fn get_sleep_cycles_with_start_filter() {
        let db = DatabaseHandler::new("sqlite::memory:").await;

        // Insert two sleep cycles
        for day in [1, 3] {
            let start = NaiveDate::from_ymd_opt(2025, 1, day)
                .unwrap()
                .and_hms_opt(22, 0, 0)
                .unwrap();
            let end = NaiveDate::from_ymd_opt(2025, 1, day + 1)
                .unwrap()
                .and_hms_opt(6, 0, 0)
                .unwrap();

            db.create_sleep(SleepCycle {
                id: end.date(),
                start,
                end,
                min_bpm: 50,
                max_bpm: 70,
                avg_bpm: 60,
                min_hrv: 30,
                max_hrv: 80,
                avg_hrv: 55,
                score: 100.0,
            })
            .await
            .unwrap();
        }

        let filter_start = NaiveDate::from_ymd_opt(2025, 1, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        let cycles = db.get_sleep_cycles(Some(filter_start)).await.unwrap();
        assert_eq!(cycles.len(), 1); // Only the Jan 3 sleep
    }
}
