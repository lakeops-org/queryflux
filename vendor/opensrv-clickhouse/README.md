# OpenSrv - ClickHouse

**Bindings for emulating a ClickHouse server.**

## Usage

See the full example [here](./examples/simple.rs)

```rust
struct Session {
    last_progress_send: Instant,
    metadata: ClickHouseMetadata,
}

#[async_trait::async_trait]
impl opensrv_clickhouse::ClickHouseSession for Session {
    async fn execute_query(&self, ctx: &mut CHContext, connection: &mut Connection) -> Result<()> {
        let query = ctx.state.query.clone();
        tracing::debug!("Receive query {}", query);

        let start = Instant::now();

        // simple logic for insert
        if query.starts_with("INSERT") || query.starts_with("insert") {
            // ctx.state.out
            let sample_block = Block::new().column("abc", Vec::<u32>::new());
            let (sender, rec) = mpsc::channel(4);
            ctx.state.out = Some(sender);
            connection.write_block(&sample_block).await?;

            let sent_all_data = ctx.state.sent_all_data.clone();
            tokio::spawn(async move {
                let mut rows = 0;
                let mut stream = ReceiverStream::new(rec);
                while let Some(block) = stream.next().await {
                    rows += block.row_count();
                    println!(
                        "got insert block: {:?}, total_rows: {}",
                        block.row_count(),
                        rows
                    );
                }
                sent_all_data.notify_one();
            });
            return Ok(());
        }

        let mut clickhouse_stream = SimpleBlockStream {
            idx: 0,
            start: 10,
            end: 24,
            blocks: 10,
        };

        while let Some(block) = clickhouse_stream.next().await {
            let block = block?;
            connection.write_block(&block).await?;

            if self.last_progress_send.elapsed() >= Duration::from_millis(10) {
                let progress = self.get_progress();
                connection
                    .write_progress(progress, ctx.client_revision)
                    .await?;
            }
        }

        let duration = start.elapsed();
        tracing::debug!(
            "ClickHouseHandler executor cost:{:?}, statistics:{:?}",
            duration,
            "xxx",
        );
        Ok(())
    }

    fn metadata(&self) -> &ClickHouseMetadata {
        &self.metadata
    }

    fn get_progress(&self) -> Progress {
        Progress {
            rows: 100,
            bytes: 1000,
            total_rows: 1000,
        }
    }
}
```

## Getting help

Submit [issues](https://github.com/datafuselabs/opensrv/issues/new/choose) for bug report or asking questions in [discussion](https://github.com/datafuselabs/opensrv/discussions/new?category=q-a).

## Credits

This project used to be [sundy-li/clickhouse-srv](https://github.com/sundy-li/clickhouse-srv).

## License

Licensed under <a href="./LICENSE">Apache License, Version 2.0</a>.
