export default {
  name: "20260715_add_trino_options",
  async run(runner) {
    const queries = [
      `ALTER TABLE saved_connection ADD COLUMN trinoOptions text not null default '{}'`,
      `ALTER TABLE used_connection ADD COLUMN trinoOptions text not null default '{}'`,
    ];
    for (let i = 0; i < queries.length; i++) {
      const query = queries[i];
      await runner.query(query);
    }
  }
}
