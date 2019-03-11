const Snowflake = require('./lib/snowflake');
const load = require('./lib/load');

const main = async (opts) => {
    const snow = new Snowflake({
        account: opts.account,
        username: opts.username,
        password: opts.password,
        region: opts.region,
        database: opts.database,
        schema: opts.schema,
        warehouse: opts.warehouse,
        role: opts.role
    });

    try {
        await snow.connect();
        await snow.prepare();

        await load({
            connection: snow,
            database: opts.database,
            schema: opts.schema,
            table: opts.table,
            bucket: opts.bucket,
            key: opts.key,
            aws_access_key: opts.aws_access_key,
            aws_secret_key: opts.aws_secret_key
        });
    } catch(ex) {
        throw ex;
    }
};

module.exports = main;