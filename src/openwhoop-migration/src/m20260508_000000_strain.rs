use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Strain::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(Strain::Id).uuid().not_null().primary_key())
                    .col(ColumnDef::new(Strain::Date).date().not_null().unique_key())
                    .col(ColumnDef::new(Strain::Strain).double().not_null())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Strain::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
#[allow(clippy::enum_variant_names)]
enum Strain {
    Table,
    Id,
    Date,
    Strain,
}
