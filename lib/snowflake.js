const snowflakeSdk = require('snowflake-sdk');

class Snowflake {
    constructor(config) {
        this.config = config;
        this.connectionSetup = snowflakeSdk.createConnection({
            account: config.account,
            username: config.username,
            password: config.password,
            region: config.region,
            database: config.database,
            schema: config.schema,
            warehouse: config.warehouse,
            role: config.role
        });
    }

    async connect() {
        return new Promise((resolve, reject) => {
            this.connectionSetup.connect((err, conn) => {
                if (err) {
                    reject(err);
                } else {
                    this.connection = conn;
                    resolve(conn);
                }
            });
        })
    }

    async prepare() {
        return new Promise((resolve, reject) => {
            if(!this.connection) {
                reject('no connection established');
            } else {
                this.connection.execute({
                    sqlText: `use warehouse ${this.config.warehouse};`,
                    complete: (err, statement, rows) => {
                        if(err) {
                            reject(err);
                        } else {
                            resolve(this.connection);
                        }
                    }
                });
            }
        })
    }

    async execute(query) {
        return new Promise((resolve, reject) => {
            this.connection.execute({
                sqlText: query,
                complete: (error, statement, rows) => {
                    if(error) {
                        reject(`${error.message}\n${statement.getSqlText()}`);
                    } else {
                        resolve(rows);
                    }
                }
            })
        })
    }
}

module.exports = Snowflake;