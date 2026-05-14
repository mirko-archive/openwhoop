use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Packets::Table)
                    .add_column(
                        ColumnDef::new(Packets::Generation)
                            .string()
                            .not_null()
                            .default("WHOOP"),
                    )
                    .to_owned(),
            )
            .await?;

        for (uuid, generation) in [
            ("61080005-8d6d-82b8-614a-1c8cb0f8dcc6", "WHOOP 4.0"),
            ("61080003-8d6d-82b8-614a-1c8cb0f8dcc6", "WHOOP 4.0"),
            ("fd4b0005-cce1-4033-93ce-002d5875f58a", "WHOOP 5.0"),
            ("fd4b0003-cce1-4033-93ce-002d5875f58a", "WHOOP 5.0"),
        ] {
            manager
                .exec_stmt(
                    Query::update()
                        .table(Packets::Table)
                        .value(Packets::Generation, generation)
                        .and_where(Expr::col(Packets::Uuid).eq(uuid))
                        .to_owned(),
                )
                .await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Packets::Table)
                    .drop_column(Packets::Generation)
                    .to_owned(),
            )
            .await
    }
}

#[derive(Iden)]
enum Packets {
    Table,
    Uuid,
    Generation,
}
