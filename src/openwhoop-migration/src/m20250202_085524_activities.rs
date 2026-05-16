use sea_orm_migration::prelude::*;

use crate::m20250127_195808_sleep_cycles::SleepCycles;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Activities::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Activities::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Activities::PeriodId).date().not_null())
                    .col(
                        ColumnDef::new(Activities::Start)
                            .date_time()
                            .not_null()
                            .unique_key(),
                    )
                    .col(ColumnDef::new(Activities::End).date_time().not_null())
                    .col(
                        ColumnDef::new(Activities::Activity)
                            .string_len(64)
                            .not_null(),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_activities_sleep_cycles")
                            .from(Activities::Table, Activities::PeriodId)
                            .to(SleepCycles::Table, SleepCycles::SleepId)
                            .on_delete(ForeignKeyAction::Cascade)
                            .on_update(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Activities::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum Activities {
    Table,
    Id,
    PeriodId,
    Start,
    End,
    Activity,
    Strain,
}
