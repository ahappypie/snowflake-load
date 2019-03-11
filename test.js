const sl = require('./index');

const main = async () => {
    const opts = {
        account: process.env.SNOWFLAKE_ACCOUNT,
        username: process.env.SNOWFLAKE_USER,
        password: process.env.SNOWFLAKE_PASSWORD,
        region: process.env.SNOWFLAKE_REGION,
        database: process.env.SNOWFLAKE_DATABASE,
        schema: process.env.SNOWFLAKE_SCHEMA,
        warehouse: process.env.SNOWFLAKE_WAREHOUSE,
        role: process.env.SNOWFLAKE_ROLE,
        table: process.env.SNOWFLAKE_TABLE,
        bucket: process.env.BUCKET,
        key: process.env.KEY,
        aws_access_key: process.env.AWS_ACCESS_KEY,
        aws_secret_key: process.env.AWS_SECRET_KEY
    };

    try {
        console.log(`Starting load: ${opts.bucket}/${opts.key} to ${opts.database}.${opts.schema}.${opts.table}`);

        await sl(opts);

        console.log(`Finished load: ${opts.bucket}/${opts.key} to ${opts.database}.${opts.schema}.${opts.table}`);
    } catch(ex) {
        console.error(ex);
    }
};

main();