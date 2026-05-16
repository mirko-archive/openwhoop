use chrono::{Days, NaiveDate, NaiveDateTime};
use openwhoop_algos::{StrainCalculator, StrainScore};
use openwhoop_entities::{heart_rate, sleep_cycles, strain};
use openwhoop_migration::OnConflict;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set};
use uuid::Uuid;

use crate::{DatabaseHandler, SearchHistory};

impl DatabaseHandler {
    pub async fn get_latest_strain(&self) -> anyhow::Result<Option<strain::Model>> {
        Ok(strain::Entity::find()
            .order_by_desc(strain::Column::Date)
            .one(&self.db)
            .await?)
    }

    pub async fn calculate_latest_strain(&self) -> anyhow::Result<()> {
        let Some(first_date) = self.get_first_reading_date().await? else {
            return Ok(());
        };
        let Some(last_date) = self.get_latest_reading_date().await? else {
            return Ok(());
        };

        let next_unsaved_date = self
            .get_latest_strain()
            .await?
            .map(|row| row.date.checked_add_days(Days::new(1)).unwrap_or(row.date))
            .unwrap_or(first_date);
        let recalc_from = last_date
            .checked_sub_days(Days::new(1))
            .unwrap_or(last_date);
        let start_date = next_unsaved_date.min(recalc_from).max(first_date);

        if start_date > last_date {
            return Ok(());
        }

        let mut date = start_date;
        while date <= last_date {
            let from = date.and_hms_opt(0, 0, 0).expect("valid start of day");
            let to = date
                .checked_add_days(Days::new(1))
                .and_then(|next| next.and_hms_opt(0, 0, 0))
                .expect("valid next day");

            let history = self
                .search_history(SearchHistory {
                    from: Some(from - chrono::TimeDelta::milliseconds(1)),
                    to: Some(to),
                    limit: None,
                })
                .await?;

            let Some(max_hr) = self.get_max_hr_before(from, to).await? else {
                date = match date.checked_add_days(Days::new(1)) {
                    Some(next) => next,
                    None => break,
                };
                continue;
            };

            let Some(resting_hr) = self.get_resting_hr_before(from).await? else {
                date = match date.checked_add_days(Days::new(1)) {
                    Some(next) => next,
                    None => break,
                };
                continue;
            };
            let calculator = StrainCalculator::new(max_hr, resting_hr);

            if let Some(StrainScore(score)) = calculator.calculate(&history) {
                self.create_or_update_strain(date, score).await?;
            }

            date = match date.checked_add_days(Days::new(1)) {
                Some(next) => next,
                None => break,
            };
        }

        Ok(())
    }

    async fn create_or_update_strain(
        &self,
        date: NaiveDate,
        score: f64,
    ) -> anyhow::Result<strain::Model> {
        let model = strain::ActiveModel {
            id: Set(Uuid::new_v4()),
            date: Set(date),
            strain: Set(score),
        };

        strain::Entity::insert(model)
            .on_conflict(
                OnConflict::column(strain::Column::Date)
                    .update_column(strain::Column::Strain)
                    .to_owned(),
            )
            .exec(&self.db)
            .await?;

        strain::Entity::find()
            .filter(strain::Column::Date.eq(date))
            .one(&self.db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("strain row missing after upsert"))
    }

    async fn get_first_reading_date(&self) -> anyhow::Result<Option<NaiveDate>> {
        Ok(self
            .get_boundary_reading_time(true)
            .await?
            .map(|time| time.date()))
    }

    async fn get_latest_reading_date(&self) -> anyhow::Result<Option<NaiveDate>> {
        Ok(self
            .get_boundary_reading_time(false)
            .await?
            .map(|time| time.date()))
    }

    async fn get_boundary_reading_time(
        &self,
        earliest: bool,
    ) -> anyhow::Result<Option<NaiveDateTime>> {
        let query = heart_rate::Entity::find().select_only();

        let query = if earliest {
            query.expr(heart_rate::Column::Time.min())
        } else {
            query.expr(heart_rate::Column::Time.max())
        };

        Ok(query.into_tuple().one(&self.db).await?)
    }

    async fn get_max_hr_before(
        &self,
        before: NaiveDateTime,
        fallback_to: NaiveDateTime,
    ) -> anyhow::Result<Option<u8>> {
        let max_hr = self
            .get_max_hr_in_range(None, before)
            .await?
            .or(self.get_max_hr_in_range(Some(before), fallback_to).await?);

        Ok(max_hr.and_then(|value| u8::try_from(value).ok()))
    }

    async fn get_max_hr_in_range(
        &self,
        from: Option<NaiveDateTime>,
        to: NaiveDateTime,
    ) -> anyhow::Result<Option<i16>> {
        let max_hr: Option<i16> = heart_rate::Entity::find()
            .select_only()
            .filter({
                let mut condition = sea_orm::Condition::all().add(heart_rate::Column::Time.lt(to));
                if let Some(from) = from {
                    condition = condition.add(heart_rate::Column::Time.gte(from));
                }
                condition
            })
            .column(heart_rate::Column::Bpm)
            .order_by_desc(heart_rate::Column::Bpm)
            .limit(1)
            .into_tuple()
            .one(&self.db)
            .await?;

        Ok(max_hr)
    }

    async fn get_resting_hr_before(&self, before: NaiveDateTime) -> anyhow::Result<Option<u8>> {
        let latest_sleep = sleep_cycles::Entity::find()
            .filter(sleep_cycles::Column::End.lt(before))
            .order_by_desc(sleep_cycles::Column::End)
            .one(&self.db)
            .await?;

        Ok(latest_sleep.and_then(|sleep| u8::try_from(sleep.min_bpm).ok()))
    }
}
