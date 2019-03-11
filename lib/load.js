const format = (d, s, t, l) => `CREATE OR REPLACE FILE FORMAT ${d}.${s}.${t}_format
type = \'CSV\' 
field_delimiter = \'${l || ','}\'
field_optionally_enclosed_by = \'\"\';`;

const stage = (d, s, t, b, aak, ask) => `CREATE OR REPLACE STAGE ${d}.${s}.${t}_stage 
file_format = ${d}.${s}.${t}_format 
url = \'s3://${b}\'
credentials = (aws_key_id=\'${aak}\' aws_secret_key=\'${ask}\');`;

const validate = (d,s,t,k) => `COPY INTO ${d}.${s}.${t}
from @${d}.${s}.${t}_stage/${k}
validation_mode = \'RETURN_ERRORS\';`;

const truncate = (d, s, t) => `TRUNCATE TABLE ${d}.${s}.${t};`;

const copy = (d, s, t, k) => `COPY INTO ${d}.${s}.${t}
from @${d}.${s}.${t}_stage/${k}
on_error = \'skip_file\';`;

const load = async (opts) => {
    const snow = opts.connection;
    await snow.execute(format(opts.database, opts.schema, opts.table));
    await snow.execute(stage(opts.database, opts.schema, opts.table, opts.bucket, opts.aws_access_key, opts.aws_secret_key));
    const errors = await snow.execute(validate(opts.database, opts.schema, opts.table, opts.key));
    if(!(errors.length > 0)) {
        await snow.execute(truncate(opts.database, opts.schema, opts.table));
        await snow.execute(copy(opts.database, opts.schema, opts.table, opts.key));
    } else {
        throw new Error(`Error validating ${opts.database}.${opts.schema}.${opts.table}: ${errors.length} errors`)
    }
};

module.exports = load;